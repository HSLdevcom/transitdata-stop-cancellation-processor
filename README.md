master: [![Build Status](https://travis-ci.org/HSLdevcom/transitdata-stop-cancellation-processor.svg?branch=master)](https://travis-ci.org/HSLdevcom/transitdata-stop-cancellation-processor)  

develop: [![Build Status](https://travis-ci.org/HSLdevcom/transitdata-stop-cancellation-processor.svg?branch=develop)](https://travis-ci.org/HSLdevcom/transitdata-stop-cancellation-processor)

# transitdata-stop-cancellation-processor

This project is part of the [Transitdata Pulsar-pipeline](https://github.com/HSLdevcom/transitdata).

## Description

Application for applying stop cancellations to trip updates that are processed by Transitdata. This has to be done in a separate application as some trip updates (e.g. trip updates for trains) are produced outside of Transitdata system and they are not processed with tripupdate-processor.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- ```mvn compile```  
- ```mvn package```  

### Docker image

- Run [this script](build-image.sh) to build the Docker image


## Running

Requirements:
- Local Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   



## Tests:

We're separating our unit & integration tests using [this pattern](https://www.petrikainulainen.net/programming/maven/integration-testing-with-maven/).

Unit tests:

- add test classes under ./src/test with suffix *Test.java
- `mvn clean test -P unit-test`   

Integration tests:

- add test classes under ./src/integration-test with prefix IT*.java
- `mvn clean verify -P integration-test`   

