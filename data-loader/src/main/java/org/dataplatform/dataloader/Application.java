package org.dataplatform.dataloader;

import com.google.api.services.storage.model.Notification;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.input.InputMessageConverter;

import static spark.Spark.port;
import static spark.Spark.post;

public class Application {

  private static final Logger LOGGER = LogManager.getLogger(Application.class);

  public static void main(String[] args) {

    DataLoaderConfig config = DataLoaderConfig.fromMap(System.getenv());
    GCSDatasourceSchemasRetriever datasourceSchemasRetriever = new GCSDatasourceSchemasRetriever(
        config.getConfigBucketName());

    port(8080);
    post("/", (req, res) -> {
      LOGGER.info("Receiving event from bucket Req body={}", req.body());

      Notification notification = InputMessageConverter.extractNotificationMessage(req.body());
      LOGGER.info("notification={}", notification);

      String filename = String
          .format("gs://%s/%s", notification.get("bucket"), notification.get("name"));
      new DataLoader(datasourceSchemasRetriever).load(filename);
      return "OK";
    });

    post("/manual", (req, res) -> {
      try {
        LOGGER.info("Receiving manual event from Req body={}", req.body());
        String filename = req.body();
        new DataLoader(datasourceSchemasRetriever).load(filename);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return "OK";
    });
  }

}
