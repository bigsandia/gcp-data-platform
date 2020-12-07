package org.dataplatform.dataloader;

import static spark.Spark.port;
import static spark.Spark.post;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.input.InputMessage;
import org.dataplatform.dataloader.input.InputMessageUtils;

public class Main {

  private static final Logger LOGGER = LogManager.getLogger(Main.class);

  public static void main(String[] args) {
    if (null == System.getenv("DATA_LOCATION")) {
      LOGGER.warn("DATA_LOCATION env var is not set. Should be set for example to US, EU, ...");
      System.exit(1);
    }


    port(8080);

    post("/", (req, res) -> {
      try {
        LOGGER.info("Req body=" + req.body());
        InputMessage inputMessage = InputMessageUtils.fromJson(req.body());
        LOGGER.info("inputMessage.inputMessage.getMessage().getData()=" + inputMessage.getMessage().getData());
        new DataLoader(inputMessage).run();
      } catch (IOException e) {
        e.printStackTrace();
      }

      return "OK";
    });
  }

}
