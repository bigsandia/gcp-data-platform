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

  private final DataLoaderConfig config;
  private final InputMessage inputMessage;

  public DataLoader(DataLoaderConfig config, InputMessage inputMessage) {
    this.inputMessage = inputMessage;
    this.config = config;
  }

  public void run() {
    DatasourceSchemasRetriever datasourceSchemasRetriever = new GCSDatasourceSchemasRetriever(
        config.getConfigBucketName()
    );
    BigQueryLoaderFactory bigQueryLoaderFactory = new BigQueryLoaderFactory();

    String messageData = inputMessage.getMessage().getData();
    String notificationAsString = new String(Base64.getDecoder().decode(messageData));
    Notification notification = new Gson().fromJson(notificationAsString, Notification.class);
    String filename = "gs://" + notification.get("bucket") + "/" + notification.get("name");

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
