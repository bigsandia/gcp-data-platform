package org.dataplatform.dataloader;

import static spark.Spark.port;
import static spark.Spark.post;

import com.google.api.services.storage.model.Notification;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.dataplatform.dataloader.input.InputMessageConverter;
import org.dataplatform.gcp.bigquery.BigQueryRepository;
import org.dataplatform.gcp.bigquery.BigQueryRepositoryImpl;

public class Application {

  private static final Logger LOGGER = LogManager.getLogger(Application.class);

  public static void main(String[] args) {

    port(8080);
    post(
        "/",
        (req, res) -> {
          String filename = "";
          ThreadContext.put("fileName", filename);

          try {
            LOGGER.info("Receiving event from bucket Req body={}", req.body());
            Notification notification = InputMessageConverter.extractNotificationMessage(req.body());
            LOGGER.info("notification={}", notification);

            filename =
                String.format("gs://%s/%s", notification.get("bucket"), notification.get("name"));
            ThreadContext.put("fileName", filename);

            DataLoaderConfig config = DataLoaderConfig.fromMap(System.getenv());
            GCSDatasourceSchemasRetriever datasourceSchemasRetriever =
                new GCSDatasourceSchemasRetriever(config.getConfigBucketName());
            BigQueryRepository bigQueryRepository = new BigQueryRepositoryImpl(config.getDataLocation());
            DataLoader dataLoader = new DataLoader(datasourceSchemasRetriever, bigQueryRepository);
            dataLoader.load(filename);
          } catch (Throwable t) {
            LOGGER.error("Error when loading file " + filename, t);
          } finally {
            ThreadContext.remove("fileName");
          }
          return "OK";
        });

    post(
        "/manual",
        (req, res) -> {
          String filename = "";
          ThreadContext.put("fileName", filename);

          try {
            LOGGER.info("Receiving manual event from Req body={}", req.body());
            filename = req.body();
            ThreadContext.put("fileName", filename);

            DataLoaderConfig config = DataLoaderConfig.fromMap(System.getenv());
            GCSDatasourceSchemasRetriever datasourceSchemasRetriever =
                new GCSDatasourceSchemasRetriever(config.getConfigBucketName());
            BigQueryRepository bigQueryRepository = new BigQueryRepositoryImpl(config.getDataLocation());
            DataLoader dataLoader = new DataLoader(datasourceSchemasRetriever, bigQueryRepository);

            dataLoader.load(filename);
          } catch (Throwable t) {
            LOGGER.error("Error when loading file " + filename + " : " + t.getMessage(), t);
          } finally {
            ThreadContext.remove("fileName");
          }
          return "OK";
        });
  }
}
