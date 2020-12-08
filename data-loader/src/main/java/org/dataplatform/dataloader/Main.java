package org.dataplatform.dataloader;

import static spark.Spark.port;
import static spark.Spark.post;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.input.InputMessage;
import org.dataplatform.dataloader.input.InputMessageUtils;

public class Main {

  private static final Logger LOGGER = LogManager.getLogger(Main.class);

  public static void main(String[] args) {
    port(8080);
    post("/", (req, res) -> {
      LOGGER.info("Req body=" + req.body());
      InputMessage inputMessage = InputMessageUtils.fromJson(req.body());

      String configBucketName = System.getenv("CONFIG_BUCKET_NAME");
      if (configBucketName == null) {
        throw new IllegalArgumentException("Missing CONFIG_BUCKET_NAME env var");
      }

      DataLoaderConfig config = new DataLoaderConfig(configBucketName);
      new DataLoader(config, inputMessage).run();

      return "OK";
    });
  }

}
