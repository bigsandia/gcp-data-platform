package org.dataplatform.dataloader.loaders;

import com.google.cloud.bigquery.LoadJobConfiguration;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.dataplatform.gcp.bigquery.BigQueryRepository;

public class BigQueryLoaderFull implements BigQueryLoader {

  private final BigQueryRepository bigQueryRepository;

  public BigQueryLoaderFull(BigQueryRepository bigQueryRepository) {
    this.bigQueryRepository = bigQueryRepository;
  }

  @Override
  public void load(String filename, DatasourceSchema datasourceSchema)
      throws BigQueryLoaderException {
    try {
      LoadJobConfiguration loadJobConfiguration =
          LoadFromGcsJobBuilder.createLoadJobFromSchema(datasourceSchema)
              .withDestinationTable(datasourceSchema.getTableId())
              .withSourceUri(filename)
              .build();

      bigQueryRepository.runJob(loadJobConfiguration, "data-loader-full-ingestion");
      bigQueryRepository.runDDLQuery(
          String.format(
              "UPDATE %s SET load_date_time = CURRENT_DATETIME() WHERE 1=1",
              datasourceSchema.getFullTableName()));
    } catch (Exception e) {
      throw new BigQueryLoaderException(
          "Cannot load file " + filename + " into " + datasourceSchema.getFullTableName(), e);
    }
  }
}
