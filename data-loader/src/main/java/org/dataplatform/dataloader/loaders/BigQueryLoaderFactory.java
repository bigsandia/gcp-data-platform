package org.dataplatform.dataloader.loaders;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.model.IngestionType;
import org.dataplatform.gcp.bigquery.BigQueryRepository;

public class BigQueryLoaderFactory {

  private static final Logger LOGGER = LogManager.getLogger(BigQueryLoaderFactory.class);
  private final BigQueryRepository bigQueryRepository;

  public BigQueryLoaderFactory(BigQueryRepository bigQueryRepository) {
    this.bigQueryRepository = bigQueryRepository;
  }

  public BigQueryLoader getLoader(IngestionType ingestionType) {
    switch (ingestionType) {
      case FULL:
        return new BigQueryLoaderFull(bigQueryRepository);
      case ADD:
        return new BigQueryLoaderAdd(bigQueryRepository);
      case DELTA:
        return new BigQueryLoaderDelta(bigQueryRepository);
      case DELTA_WITH_HISTORIC:
        LOGGER.info("Delta with historic");
    }

    throw new RuntimeException("Ingestion type does not exist ~ it should never happen !");
  }

}
