# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Maven-based Spring Boot project demonstrating TestContainers integration with Apache Pinot. The project provides a complete test harness for Apache Pinot using Docker containers.

## Key Commands

### Building and Testing
- **Build**: `mvn clean package`
- **Run tests**: `mvn test`
- **Run single test**: `mvn test -Dtest=ClassName`
- **Run application**: `mvn spring-boot:run`

### Requirements
- Java 21
- Maven 3.6+  
- Docker (for TestContainers)

## Architecture

### Core Components

**ApachePinotCluster** (`src/test/java/org/apache/pinot/tc/ApachePinotCluster.java`)
- TestContainers implementation that orchestrates a complete Pinot cluster
- Sets up ZooKeeper, Controller, Broker, Server, and optional Minion containers
- Manages networking and container lifecycle
- Entry point for all Pinot testing scenarios

**Service Layer**
- `BrokerService`: Executes SQL queries against Pinot broker (port 8099)
- `ControllerService`: Manages schemas, tables, and data ingestion via controller (port 9000)

**Test Resources**
- `src/test/resources/`: Contains sample schemas, table configs, and CSV data
- Used by integration tests to demonstrate complete Pinot workflows

### Container Network Architecture
- All containers communicate via Docker network
- ZooKeeper acts as coordination service
- Controller manages cluster metadata
- Broker handles query routing
- Server stores and processes data
- Minion performs background tasks (optional)

### WebClient Configuration
- Configured in `WebClientConfig` with separate clients for broker and controller
- Uses Spring WebFlux for non-blocking HTTP operations

## Test Structure

Tests follow pattern of:
1. Start ApachePinotCluster
2. Create schema via ControllerService  
3. Create table via ControllerService
4. Ingest data via ControllerService
5. Query data via BrokerService
6. Verify results

## Package Structure
- `api/`: Response models and exceptions
- `config/`: Spring configuration classes  
- Root package: Main services and application entry point