package org.dataplatform.dataloader;

public class DataLoaderConfig {

  private final String configBucketName;

  public DataLoaderConfig(String configBucketName) {
    this.configBucketName = configBucketName;
  }

  public String getConfigBucketName() {
    return configBucketName;
  }
}
