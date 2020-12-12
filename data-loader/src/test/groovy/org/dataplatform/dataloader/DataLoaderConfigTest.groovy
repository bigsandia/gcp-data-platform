package org.dataplatform.dataloader


import spock.lang.Specification

class DataLoaderConfigTest extends Specification {

    def "should load config from map"() {
        given:
        Map<String, String> keyValueConfig = Map.of(
                "CONFIG_BUCKET_NAME", "gs://aaaa",
                "DATA_LOCATION", "US"
        )

        when:
        DataLoaderConfig dataLoaderConfig = DataLoaderConfig.fromMap(keyValueConfig)


        then:
        dataLoaderConfig.getConfigBucketName() == "gs://aaaa"
        dataLoaderConfig.getDataLocation() == "US"
    }

    def "should throw error when missing property"() {
        given:
        Map<String, String> keyValueConfig = Map.of(
                "CONFIG_BUCKET_NAME", "gs://aaaa",
        )

        when:
        DataLoaderConfig dataLoaderConfig = DataLoaderConfig.fromMap(keyValueConfig)


        then:
        final IllegalArgumentException exception = thrown()
        exception.message =="Missing DATA_LOCATION configuration"
    }
}
