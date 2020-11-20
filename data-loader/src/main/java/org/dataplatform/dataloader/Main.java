package org.dataplatform.dataloader;

import java.io.IOException;
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
  public Object call() throws IOException {
    return 0;
  }

}
