package org.dataplatform.dataloader;

import com.google.api.services.storage.model.Notification;
import com.google.gson.Gson;
import java.util.Base64;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.input.InputMessage;
import org.dataplatform.dataloader.loaders.BigQueryLoader;
import org.dataplatform.dataloader.loaders.BigQueryLoaderException;
import org.dataplatform.dataloader.loaders.BigQueryLoaderFactory;
import org.dataplatform.dataloader.model.DatasourceSchema;

public class DataLoader {

  private static final Logger LOGGER = LogManager.getLogger(DataLoader.class);

  private DatasourceSchemasRetriever datasourceSchemasRetriever;

  public DataLoader( DatasourceSchemasRetriever datasourceSchemasRetriever) {
    this.datasourceSchemasRetriever = datasourceSchemasRetriever;
  }

  public void run(Notification notification) {
    BigQueryLoaderFactory bigQueryLoaderFactory = new BigQueryLoaderFactory();

    String filename = String.format("gs://%s/%s", notification.get("bucket"), notification.get("name"));

    LOGGER.info("Loading file {}", filename);

    List<DatasourceSchema> datasourceSchemas = datasourceSchemasRetriever
        .findCorrespondingSchemas(filename);

    for (DatasourceSchema schema : datasourceSchemas) {
      BigQueryLoader loader = bigQueryLoaderFactory.getLoader(schema.getIngestionType());
      try {
        loader.load(filename, schema);
      } catch (BigQueryLoaderException e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
  }

}
