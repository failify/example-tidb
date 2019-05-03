This repo contains sample [Failify](https://failify.io) test cases for TiDB. To make things easier for illustration
purposes, the bin folder from v2.1.8 of TiDB built on Debian Stretch is included in the repo so anyone who wants to
try the test cases is not required to build TiDB from scratch.

To run the test cases, first you need to install the following dependencies:
- Java 8
- Docker 1.13+
- Maven 3.x

Also make sure you can run docker commands with the user you are running the tests with. For more information check
[Failify documentation](https://docs.failify.io).

After installing all the dependencies, you can run the test cases using:

```console
$ cd failify
$ mvn test
```