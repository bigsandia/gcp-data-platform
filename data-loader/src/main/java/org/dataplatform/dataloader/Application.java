package org.dataplatform.dataloader;

import static spark.Spark.port;
import static spark.Spark.post;

import com.google.api.services.storage.model.Notification;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.input.InputMessageConverter;
import org.dataplatform.dataloader.loaders.BigQueryLoaderException;
import org.dataplatform.gcp.bigquery.BigQueryRepository;
import org.dataplatform.gcp.bigquery.BigQueryRepositoryImpl;

public class Application {

  private static final Logger LOGGER = LogManager.getLogger(Application.class);

  public static void main(String[] args) {

    DataLoaderConfig config = DataLoaderConfig.fromMap(System.getenv());
    GCSDatasourceSchemasRetriever datasourceSchemasRetriever =
        new GCSDatasourceSchemasRetriever(config.getConfigBucketName());
    BigQueryRepository bigQueryRepository = new BigQueryRepositoryImpl(config.getDataLocation());
    DataLoader dataLoader = new DataLoader(datasourceSchemasRetriever, bigQueryRepository);

            LOGGER.info("Receiving event from bucket Req body={}", req.body());

            Notification notification = InputMessageConverter.extractNotificationMessage(req.body());
            LOGGER.info("notification={}", notification);

            String filename =
                String.format("gs://%s/%s", notification.get("bucket"), notification.get("name"));

            dataLoader.load(filename);
          } catch (Throwable t) {
            LOGGER.error(t);
          }
          return "OK";
        });

    post(
        "/manual",
        (req, res) -> {
          try {
            LOGGER.info("Receiving manual event from Req body={}", req.body());
            String filename = req.body();
            dataLoader.load(filename);
          } catch (Throwable t) {
            LOGGER.error(t);
          }
          return "OK";
        });
  }
}
