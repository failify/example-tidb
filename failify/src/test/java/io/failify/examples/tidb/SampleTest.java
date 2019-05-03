package io.failify.examples.tidb;


import io.failify.FailifyRunner;
import io.failify.dsl.entities.Deployment;
import io.failify.exceptions.RuntimeEngineException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.concurrent.TimeoutException;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);

    protected static FailifyRunner runner;

    @BeforeClass
    public static void before() throws RuntimeEngineException, TimeoutException {
        Deployment deployment = FailifyHelper.getDeployment(3, 2 ,1);
        runner = FailifyRunner.run(deployment);
        FailifyHelper.startNodesInOrder(runner, 3);
        logger.info("The cluster is UP!");
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }


    @Test
    public void sampleTest() throws RuntimeEngineException, SQLException, ClassNotFoundException, TimeoutException {
        try (Connection conn = FailifyHelper.getTiDBConnection(runner)) {
            Statement s = conn.createStatement();
            s.executeUpdate("CREATE DATABASE tidb_test");
            s.executeQuery("USE tidb_test");
            s.executeUpdate("CREATE TABLE person (id int, name varchar(255))");
            s.executeUpdate("INSERT INTO person (id, name) VALUES (1, 'armin')");

            ResultSet result = s.executeQuery("SELECT * FROM person");
            result.next();
            assertEquals(result.getInt("id"), 1);
            assertEquals(result.getString("name"), "armin");

            s.executeUpdate("DROP DATABASE tidb_test");
        }

    }
}
