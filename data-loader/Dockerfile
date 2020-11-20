FROM    openjdk:11-jre-slim

COPY    target/*-jar-with-dependencies.jar /app/app.jar

ENTRYPOINT  ["java", "-XX:MinRAMPercentage=50", "-XX:MaxRAMPercentage=95", "-jar", "/app/app.jar"]
