# Apache Pinot TestContainers Example

[![CI](https://github.com/timveil-startree/pinot-testcontainers-example/actions/workflows/ci.yml/badge.svg)](https://github.com/timveil-startree/pinot-testcontainers-example/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.4-brightgreen.svg)](https://spring.io/projects/spring-boot)

This project demonstrates how to use [TestContainers](https://www.testcontainers.org/) with [Apache Pinot](https://pinot.apache.org/) for integration testing. It provides a Spring Boot application that interacts with Apache Pinot and includes TestContainers-based tests to verify the functionality.

## Overview

Apache Pinot is a real-time distributed OLAP datastore designed for low-latency, high-throughput analytics. This project showcases how to:

1. Set up a complete Apache Pinot cluster using TestContainers
2. Create schemas and tables in Pinot
3. Ingest data into Pinot
4. Execute SQL queries against Pinot
5. Test all of the above in an automated way

## Prerequisites

- Java 21 or higher
- Maven 3.6 or higher
- Docker (required for TestContainers)

## Project Structure

```
├── src
│   ├── main
│   │   ├── java
│   │   │   └── org/apache/pinot/tc
│   │   │       ├── api                  # API models
│   │   │       ├── config               # Configuration classes
│   │   │       ├── BatchIngestConfiguration.java  # Configuration for batch data ingestion
│   │   │       ├── BrokerService.java   # Service for interacting with Pinot Broker
│   │   │       ├── ControllerService.java  # Service for interacting with Pinot Controller
│   │   │       └── PinotServicesApplication.java  # Spring Boot application
│   │   └── resources
│   │       └── application.properties   # Application configuration
│   └── test
│       ├── java
│       │   └── org/apache/pinot/tc
│       │       ├── ApachePinotCluster.java  # TestContainers implementation for Pinot
│       │       ├── ApachePinotClusterTest.java  # Simple test for the Pinot cluster
│       │       ├── BasicPinotTests.java  # Integration tests for Pinot
│       │       ├── BasicPinotJDBCTests.java  # JDBC-based tests for Pinot
│       │       └── MinionTests.java  # Tests for Pinot Minion
│       └── resources
│           ├── transcript-schema.json  # Sample schema for testing
│           ├── transcript-table-offline.json  # Sample table config for testing
│           ├── transcript-table-offline-minion.json  # Sample table config with Minion
│           └── transcripts.csv  # Sample data for testing
```

## Key Components

### ApachePinotCluster

The `ApachePinotCluster` class is a TestContainers implementation that sets up a complete Pinot cluster for testing, including:

- ZooKeeper container
- Pinot Controller container
- Pinot Broker container
- Pinot Server container
- Optional Pinot Minion container

### Service Classes

- `BrokerService`: Provides methods to execute SQL queries against the Pinot broker
- `ControllerService`: Provides methods to create schemas, tables, and ingest data

### Test Classes

- `BasicPinotTests`: Demonstrates how to use the services to interact with Pinot in tests
- `BasicPinotJDBCTests`: Shows how to use JDBC to connect to Pinot
- `MinionTests`: Demonstrates how to use Pinot Minion for tasks like purging data

## Usage

### Running the Tests

To run the tests, use the following Maven command:

```bash
mvn test
```

### Example: Setting Up a Pinot Cluster

```java
// Create a new network for the containers
Network network = Network.newNetwork();

// Create a Pinot cluster with ZooKeeper 3.9 and Pinot latest
ApachePinotCluster cluster = new ApachePinotCluster(
    "zookeeper:3.9",
        "apachepinot/pinot:1.4.0-21-ms-openjdk",
        false,  // enableMinion
    network
);

// Start the cluster
cluster.start();

// Use the cluster...

// Stop the cluster when done
cluster.stop();
```

### Example: Creating a Schema and Table

```java
// Create a schema
PostResponse schemaResponse = controllerService.createSchema(schemaResource);

// Create a table
PostResponse tableResponse = controllerService.createTable(tableResource);
```

### Example: Ingesting Data

```java
// Ingest data from a CSV file
PostResponse ingestResponse = controllerService.ingestFromFile(
    "tableName_OFFLINE",
    new BatchIngestConfiguration("csv", ","),
    dataResource
);
```

### Example: Executing a Query

```java
// Execute a SQL query
QueryResponse queryResponse = brokerService.executeQuery("SELECT * FROM tableName");
```

## Configuration

The application can be configured using the `application.properties` file:

```properties
# Logging
logging.level.root=WARN
logging.level.org.testcontainers=INFO
logging.level.org.apache.pinot.tc=DEBUG
```

## Building the Project

To build the project, use the following Maven command:

```bash
mvn clean package
```

## License

This project is licensed under the Apache License 2.0.

## Resources

- [Apache Pinot Documentation](https://docs.pinot.apache.org/)
- [TestContainers Documentation](https://www.testcontainers.org/)
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)