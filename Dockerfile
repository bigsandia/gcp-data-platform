FROM maven:3.6-jdk-11 as builder

# Copy local code to the container image.
WORKDIR /app
COPY pom.xml .
COPY . ./

ARG module
ARG it_test_sa_json

RUN echo $it_test_sa_json > /app/it_test_sa.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/it_test_sa.json

# Build a release artifact.
RUN mvn verify -pl commons,$module

# Use AdoptOpenJDK for base image.
# It's important to use OpenJDK 8u191 or above that has container support enabled.
# https://hub.docker.com/r/adoptopenjdk/openjdk8
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM adoptopenjdk/openjdk11:alpine-slim

ARG module

WORKDIR /app

COPY ./$module/target/*-jar-with-dependencies.jar /app/app.jar

CMD ["java", "-jar", "/app/app.jar"]
