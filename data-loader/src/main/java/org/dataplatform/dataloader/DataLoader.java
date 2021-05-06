package org.dataplatform.dataloader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.exceptions.DatasourceSchemaAlreadyInProcessException;
import org.dataplatform.dataloader.enricher.BigqueryTableMetadataEnricher;
import org.dataplatform.dataloader.loaders.BigQueryLoader;
import org.dataplatform.dataloader.loaders.BigQueryLoaderException;
import org.dataplatform.dataloader.loaders.BigQueryLoaderFactory;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.dataplatform.gcp.bigquery.BigQueryRepository;

import java.util.List;

public class DataLoader {

  private static final Logger LOGGER = LogManager.getLogger(DataLoader.class);

  private final DatasourceSchemasRetriever datasourceSchemasRetriever;
  private final BigQueryLoaderFactory bigQueryLoaderFactory;
  private final BigqueryTableMetadataEnricher bigQueryTableMetadataEnricher;

  public DataLoader(
      DatasourceSchemasRetriever datasourceSchemasRetriever,
      BigQueryRepository bigQueryRepository) {
    this.datasourceSchemasRetriever = datasourceSchemasRetriever;
    bigQueryLoaderFactory = new BigQueryLoaderFactory(bigQueryRepository);
    bigQueryTableMetadataEnricher = new BigqueryTableMetadataEnricher(bigQueryRepository);
  }

  public void load(String filename) throws BigQueryLoaderException, DatasourceSchemaAlreadyInProcessException {

    LOGGER.info("Loading file {}", filename);

    List<DatasourceSchema> datasourceSchemas =
        datasourceSchemasRetriever.findCorrespondingSchemas(filename);

    if (FileInProcessMap.isInProcess(datasourceSchemas)) {
      String rawPath = datasourceSchemas.get(0).getRawPath();
      String filenameBeingProcessed = FileInProcessMap.getFileNameInProcess(rawPath);
      throw new DatasourceSchemaAlreadyInProcessException("The schema with rawPath " + rawPath + " is already currently being processed for file" + filenameBeingProcessed );
    }

    FileInProcessMap.addInProcess(filename, datasourceSchemas);

    try {
      for (DatasourceSchema schema : datasourceSchemas) {
        BigQueryLoader loader = bigQueryLoaderFactory.getLoader(schema.getIngestionType());
        loader.load(filename, schema);
        bigQueryTableMetadataEnricher.enrich(schema);
      }
    } catch (Throwable t) {
      throw t;
    } finally {
      FileInProcessMap.removeInProcess(datasourceSchemas);
    }
  }
}
