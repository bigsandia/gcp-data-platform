package org.dataplatform.dataloader;

import com.google.common.base.Strings;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class DataLoaderConfig {

    private final String configBucketName;
    private final String dataLocation;

    private DataLoaderConfig(String configBucketName, String dataLocation) {
        this.configBucketName = configBucketName;
        this.dataLocation = dataLocation;
    }

    public static DataLoaderConfig fromMap(Map<String, String> keyValuesConfig) {
        return new DataLoaderConfig(
                mandatoryConfig("CONFIG_BUCKET_NAME", keyValuesConfig),
                mandatoryConfig("DATA_LOCATION", keyValuesConfig));
    }

    private static String mandatoryConfig(String propertyKey, Map<String, String> keyValuesConfig) {
        String value = keyValuesConfig.get(propertyKey);
        checkArgument(!Strings.isNullOrEmpty(value), "Missing %s configuration", propertyKey);
        return value;
    }

    public String getConfigBucketName() {
        return configBucketName;
    }

    public String getDataLocation() {
        return dataLocation;
    }
}
