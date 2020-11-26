package org.dataplatform.dataloader.loaders;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.model.IngestionType;

public class BigQueryLoaderFactory {

  private static final Logger LOGGER = LogManager.getLogger(BigQueryLoaderFactory.class);

  public BigQueryLoader getLoader(IngestionType ingestionType) {
    switch (ingestionType) {
      case FULL:
        return new BigQueryLoaderFull();
      case ADD:
        return new BigQueryLoaderAdd();
      case DELTA:
        return new BigQueryLoaderDelta();
      case DELTA_WITH_HISTORIC:
        LOGGER.info("Delta with historic");
    }

    throw new RuntimeException("Ingestion type does not exist ~ it should never happen !");
  }

}
