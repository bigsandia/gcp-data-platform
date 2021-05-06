FROM adoptopenjdk/openjdk11:alpine-slim

ARG module

WORKDIR /app

COPY ./$module/target/*-jar-with-dependencies.jar /app/app.jar

CMD ["java", "-jar", "/app/app.jar"]
