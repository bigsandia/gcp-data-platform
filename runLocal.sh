mvn clean package
LOG4J_CONFIGURATION_FILE=log4j2_local.xml CONFIG_BUCKET_NAME=another-data-platform-config DATA_LOCATION=US java -jar data-loader/target/data-loader-1.0.0-jar-with-dependencies.jar

