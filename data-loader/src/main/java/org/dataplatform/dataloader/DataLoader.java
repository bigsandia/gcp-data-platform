package org.dataplatform.dataloader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.loaders.BigQueryLoader;
import org.dataplatform.dataloader.loaders.BigQueryLoaderException;
import org.dataplatform.dataloader.loaders.BigQueryLoaderFactory;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.dataplatform.gcp.bigquery.BigQueryRepository;

import java.util.List;

public class DataLoader {

  private static final Logger LOGGER = LogManager.getLogger(DataLoader.class);

  private DatasourceSchemasRetriever datasourceSchemasRetriever;
  private final BigQueryLoaderFactory bigQueryLoaderFactory;

  public DataLoader(
      DatasourceSchemasRetriever datasourceSchemasRetriever,
      BigQueryRepository bigQueryRepository) {
    this.datasourceSchemasRetriever = datasourceSchemasRetriever;
    bigQueryLoaderFactory = new BigQueryLoaderFactory(bigQueryRepository);
  }

  public void load(String filename) throws BigQueryLoaderException {

    LOGGER.info("Loading file {}", filename);

    List<DatasourceSchema> datasourceSchemas =
        datasourceSchemasRetriever.findCorrespondingSchemas(filename);

    for (DatasourceSchema schema : datasourceSchemas) {
      BigQueryLoader loader = bigQueryLoaderFactory.getLoader(schema.getIngestionType());
      loader.load(filename, schema);
    }
  }
}
