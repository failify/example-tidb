package io.failify.examples.tidb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.failify.FailifyRunner;
import io.failify.dsl.entities.Deployment;
import io.failify.dsl.entities.PathAttr;
import io.failify.dsl.entities.PortType;
import io.failify.exceptions.RuntimeEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeoutException;

public class FailifyHelper {
    public static final Logger logger = LoggerFactory.getLogger(FailifyHelper.class);

    public static Deployment getDeployment(int numOfPds, int numOfKvs, int numOfDbs) {
        String pdStr = getPdString(numOfPds, false);
        String pdInitialClusterStr = getPdString(numOfPds, true);
        return Deployment.builder("example-tidb")
                // Service Definitions
                .withService("tidb")
                    .appPath("../tidb-2.1.8-bin", "/tidb")
                    .dockerImgName("failify/tidb:1.0").dockerFileAddr("docker/Dockerfile", false)
                    .startCmd("/tidb/tidb-server --store=tikv --path=\"" + pdStr + "\"").tcpPort(4000)
                .and().nodeInstances(numOfDbs, "tidb", "tidb", true)
                .withService("tikv")
                    .appPath("../tikv-2.1.8-bin", "/tikv").disableClockDrift()
                    .startCmd("/tikv/tikv-server --addr=\"0.0.0.0:20160\"  --advertise-addr=\"$(hostname):20160\" " +
                            "--pd=\"" + pdStr + "\" --data-dir=/data")
                    .dockerImgName("failify/tidb:1.0").dockerFileAddr("docker/Dockerfile", false)
                .and().nodeInstances(numOfKvs, "tikv", "tikv", true)
                .withService("pd")
                    .appPath("../pd-2.1.8-bin", "/pd")
                    .startCmd("/pd/pd-server --name=\"$(hostname)\" --client-urls=\"http://0.0.0.0:2379\" --data-dir=/data " +
                            "--advertise-client-urls=\"http://$(hostname):2379\" --peer-urls=\"http://0.0.0.0:2380\" " +
                            "--advertise-peer-urls=\"http://$(hostname):2380\" --initial-cluster=\"" + pdInitialClusterStr + "\"")
                    .dockerImgName("failify/tidb:1.0").dockerFileAddr("docker/Dockerfile", false).tcpPort(2379)
                .and().nodeInstances(numOfPds, "pd", "pd", false).build();
    }

    private static String getPdString(int numOfPds, boolean http) {
        StringJoiner joiner = new StringJoiner(",");
        for (int i=1; i<=numOfPds; i++) {
            joiner.add("pd" + i + (http ? "=http://pd" + i + ":2380" : ":2379"));
        }
        return joiner.toString();
    }

    public static void startNodesInOrder(FailifyRunner runner, int numOfPds) throws RuntimeEngineException, TimeoutException {
        logger.info("Waiting for PDs to get online ...");
        int attemptCounter = 0;
        while (true) {
            HttpResponse<String> response = null;
            try {
                Unirest.setTimeouts(1000,5000);
                response = Unirest.get("http://" + runner.runtime().ip("pd1") + ":" +
                        runner.runtime().portMapping("pd1", 2379, PortType.TCP) + "/pd/api/v1/members").asString();
            } catch (UnirestException e) {
                logger.debug("Error while getting the members of the PD cluster");
            }
            if (response.getStatus() == 200) {
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    if (((List)objectMapper.readValue(response.getBody(), Map.class).get("members")).size() == numOfPds) {
                        break;
                    }
                } catch (IOException e) {
                    logger.debug("error while parsing PD members json data {}", response.getBody());
                }
            }
            if (++attemptCounter >= 10) {
                throw new TimeoutException("Waiting for PD cluster is timeouted!");
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                logger.debug("The PD startup wait thread got interrupted");
            }
        }
        logger.info("PDs are online!");

        for (String nodeName: runner.runtime().nodeNames()) {
            if (nodeName.startsWith("tikv")) {
                runner.runtime().startNode(nodeName);

            }
        }

        for (String nodeName: runner.runtime().nodeNames()) {
            if (nodeName.startsWith("tidb")) {
                runner.runtime().startNode(nodeName);

            }
        }

        logger.info("Waiting for the rest of the cluster to get online ...");
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static Connection getTiDBConnection(FailifyRunner runner) throws ClassNotFoundException, TimeoutException, SQLException {
        StringJoiner hostStr = new StringJoiner(",");
        for (String node: runner.runtime().nodeNames()) {
            if (node.startsWith("tidb")) {
                hostStr.add(runner.runtime().ip(node) + ":" + runner.runtime().portMapping(node, 4000, PortType.TCP));
            }
        }
        Class.forName("com.mysql.jdbc.Driver");
        String connStr = "jdbc:mysql://" + hostStr +
                "/?user=root&useUnicode=yes&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false&maxReconnects=10";
        return DriverManager.getConnection(connStr);
    }
}
