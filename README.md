# Kafka Subscription Service

This is the service providing the endpoints to subscribe/unsubscribe to Kafka Streams of Publishers

## Prerequisites

#### Offline
* Java 8
* Maven 3.1+

## Setting up the Subscription Service
* Clone the git repository
* Go to the project directory and perform ‘mvn clean package‘

## Starting the Subscription Service
* Run ‘java -jar target/kafka-subscription-0.1.0-SNAPSHOT-jar-with-dependencies.jar‘

## Using Docker Containers to run the application
* Make sure Docker is available on your machine
* Run ‘mvn docker:build‘
* Run ‘docker-compose up -d kafka-subscription‘ 