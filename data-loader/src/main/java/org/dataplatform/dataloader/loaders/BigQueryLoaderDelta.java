package org.dataplatform.dataloader.loaders;

import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.GcsFileToBqTableLoader;
import org.dataplatform.dataloader.model.Column;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.dataplatform.gcp.bigquery.BigQueryRepository;

public class BigQueryLoaderDelta implements BigQueryLoader {

  public static Logger LOGGER = LogManager.getLogger(BigQueryLoaderDelta.class);

  private final BigQueryRepository bigQueryRepository;

  public BigQueryLoaderDelta(BigQueryRepository bigQueryRepository) {
    this.bigQueryRepository = bigQueryRepository;
  }

  @Override
  public void load(String filepath, DatasourceSchema datasourceSchema)
      throws BigQueryLoaderException {

    if (!bigQueryRepository.tableExists(datasourceSchema.getTableId())) {
      LOGGER.info("Table not exists start a full loading");
      new BigQueryLoaderFull(bigQueryRepository).load(filepath, datasourceSchema);
    } else {
      // 1- charger dans une table temporaire
      GcsFileToBqTableLoader gcsFileToBqTableLoader = new GcsFileToBqTableLoader(
          bigQueryRepository, filepath, datasourceSchema);
      gcsFileToBqTableLoader.load();

      String query =
          "MERGE\n"
              + "  `"
              + datasourceSchema.getFullTableName()
              + "` dest\n"
              + "USING\n"
              + "  `"
              + datasourceSchema.getFullTableTmpName()
              + "` src\n"
              + "ON\n"
              + "  "
              + primaryKeys(datasourceSchema)
              + "\n"
              + "WHEN NOT MATCHED THEN\n"
              + "  INSERT "
              + insertClause(datasourceSchema)
              + "\n"
              + "WHEN MATCHED THEN\n"
              + "  UPDATE SET "
              + updateClause(datasourceSchema);
      try {
        bigQueryRepository.runDDLQuery(query);

      } catch (Exception e) {
        throw new BigQueryLoaderException(
            "Cannot merge from "
                + datasourceSchema.getFullTableTmpName()
                + " into destination table "
                + datasourceSchema.getFullTableName(),
            e);
      }
      bigQueryRepository.dropTable(datasourceSchema.getTmpTableId());
    }
  }

  private String primaryKeys(DatasourceSchema datasourceSchema) {
    return datasourceSchema.getColumns().stream()
        .filter(Column::isPrimaryKey)
        .map(column -> castIfNecessary(column, "src") + " = dest." + column.getName())
        .collect(Collectors.joining(" AND "));
  }

  private String insertClause(DatasourceSchema datasourceSchema) {
    String columns =
        datasourceSchema.getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.joining(", "))
            + ", load_date_time";
    String values =
        datasourceSchema.getColumns().stream()
                .map(column -> {
                  return castIfNecessary(column, "");
                })
                .collect(Collectors.joining(", "))
            + ", CURRENT_DATETIME()";

    return "(" + columns + ") VALUES (" + values + ")";
  }

  private String updateClause(DatasourceSchema datasourceSchema) {
    return datasourceSchema.getColumns().stream()
        .filter(column -> !column.isPrimaryKey())
        .map(column -> column.getName() + " = " + castIfNecessary(column, "src"))
        .collect(Collectors.joining(", "));
  }

  private String castIfNecessary(Column column, String optionalTable) {
    String prefix = "";
    if (optionalTable != null && !optionalTable.trim().equals("")) {
      prefix = optionalTable + ".";
    }
    if (column.columnWithDateConversion()) {
      return String
          .format("PARSE_DATE(\"%s\", %s)", column.getPattern(), prefix + column.getName());
    }
    return prefix + column.getName();
  }
}
