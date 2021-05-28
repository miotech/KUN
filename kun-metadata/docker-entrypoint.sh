#!/usr/bin/env sh
dockerize -wait http://${INFRA_HOST}:${INFRA_PORT}/health -wait-retry-interval 3s -timeout 180s
java ${JVM_OPTS} -jar /server/target/kun-metadata-web-1.0.jar
