package org.dataplatform.dataloader;

import static spark.Spark.port;
import static spark.Spark.post;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.input.InputMessage;

public class Main {

  private static final Logger LOGGER = LogManager.getLogger(Main.class);

  public static void main(String[] args) {
    port(8080);

    post("/", (req, res) -> {
      LOGGER.info("Req body=" + req.body());
      InputMessage inputMessage = new ObjectMapper().readValue(req.body(), InputMessage.class);
      new DataLoader(inputMessage).run();

      return "OK";
    });
  }

}
