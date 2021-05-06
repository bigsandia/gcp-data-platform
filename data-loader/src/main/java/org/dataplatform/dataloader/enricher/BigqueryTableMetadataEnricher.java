package org.dataplatform.dataloader.enricher;

import com.google.cloud.bigquery.TableId;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.dataplatform.gcp.bigquery.BigQueryRepository;

public class BigqueryTableMetadataEnricher {

  public static Logger LOGGER = LogManager.getLogger(BigqueryTableMetadataEnricher.class);

  private final BigQueryRepository bigQueryRepository;

  public BigqueryTableMetadataEnricher(BigQueryRepository bigQueryRepository) {
    this.bigQueryRepository = bigQueryRepository;
  }

  public void enrich(DatasourceSchema schema) {
    TableId tableId = TableId.of(schema.getProject(), schema.getDataset(), schema.getTable());
    bigQueryRepository.setTableDescription(tableId, schema.getDescription());
    bigQueryRepository.setTableLabels(tableId, schema.getLabels());

    Map<String, String> descriptionByField = schema.getColumns().stream()
        .map(column -> Map.entry(column.getName(), column.getDescription() != null ? column.getDescription() : ""))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    bigQueryRepository.setFieldsDescription(tableId, descriptionByField);
    LOGGER.debug("Successfully updated description, labels and fields description of table {}", tableId);
  }

}
