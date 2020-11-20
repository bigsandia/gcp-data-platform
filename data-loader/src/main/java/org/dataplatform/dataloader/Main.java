package org.dataplatform.dataloader;

import static spark.Spark.*;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "data-loader", mixinStandardHelpOptions = true, version = "")
public class Main implements Callable {

  private static final Logger LOGGER = LogManager.getLogger(Main.class);

  public static void main(String[] args) {
    LOGGER.info("Parameters : {}", Arrays.toString(args));
    new CommandLine(new Main()).execute(args);
  }

  @Override
  public Object call() {

    port(8080);
    path("/v1", () -> {
      get("/table/:tableName/events", (req, res) -> {
        System.out.println("received parameter tableName=" + req.params(":tableName"));
        return "hello world";
      });
    });

    return 0;
  }

}
