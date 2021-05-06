package org.dataplatform.dataloader.loaders;

import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.QueryJobConfiguration;
import java.util.stream.Collectors;
import org.dataplatform.dataloader.GcsFileToBqTableLoader;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.dataplatform.gcp.bigquery.BigQueryRepository;

public class BigQueryLoaderFull implements BigQueryLoader {

  private final BigQueryRepository bigQueryRepository;

  public BigQueryLoaderFull(BigQueryRepository bigQueryRepository) {
    this.bigQueryRepository = bigQueryRepository;
  }

  @Override
  public void load(String filename, DatasourceSchema datasourceSchema)
      throws BigQueryLoaderException {
    try {
      // 1- charger dans une table temporaire
      GcsFileToBqTableLoader gcsFileToBqTableLoader = new GcsFileToBqTableLoader(
          bigQueryRepository, filename, datasourceSchema);
      gcsFileToBqTableLoader.load();

      // 2 - insérer dans la table finale (transformation des colonnes + ajout load_date_time)
      QueryJobConfiguration jobConfig = QueryJobConfiguration
          .newBuilder("SELECT " + selectClauseWithTransformedColumns(datasourceSchema) +
              ", CURRENT_DATETIME() as load_date_time"
              + " FROM " + datasourceSchema.getFullTableTmpName())
          .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
          .setDestinationTable(datasourceSchema.getTableId())
          .build();
      bigQueryRepository.runJob(jobConfig, "data-loader");
    } catch (Exception e) {
      throw new BigQueryLoaderException(
          "Cannot load file " + filename + " into " + datasourceSchema.getFullTableName(), e);
    }
  }

  private String selectClauseWithTransformedColumns(DatasourceSchema datasourceSchema) {
    return
        datasourceSchema.getColumns().stream()
            .map(column -> {
              if (column.columnWithDateConversion()) {
                return String.format("PARSE_DATE(\"%s\", %s) AS %s", column.getPattern(), column.getName(), column.getName());
              }
              return column.getName();
            })
            .collect(Collectors.joining(", "));
  }
}
