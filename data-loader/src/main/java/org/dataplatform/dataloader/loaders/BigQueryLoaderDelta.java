package org.dataplatform.dataloader.loaders;

import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.GcsFileToBqTableLoader;
import org.dataplatform.dataloader.model.Column;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.dataplatform.gcp.bigquery.BigQueryRepository;
import org.dataplatform.gcp.bigquery.BigQueryRepositoryImpl;

public class BigQueryLoaderDelta implements BigQueryLoader {

  public static Logger LOGGER = LogManager.getLogger(BigQueryLoaderDelta.class);

  @Override
  public void load(String filename, DatasourceSchema datasourceSchema)
      throws BigQueryLoaderException {
    BigQueryRepository bqRepo = new BigQueryRepositoryImpl();

    // 1- charger dans une table temporaire
    try {
      GcsFileToBqTableLoader gcsFileToBqTableLoader = new GcsFileToBqTableLoader(
          bqRepo, filename, datasourceSchema);
      gcsFileToBqTableLoader.load();
    } catch (Exception e) {
      throw new BigQueryLoaderException(
          "Cannot load file " + filename + " into " + datasourceSchema.getFullTableTmpName(), e);
    }

    // 2- crée la table si elle n'existe pas
    if (bqRepo.tableExists(datasourceSchema.getTableId())) {
      LOGGER.info("Table " + datasourceSchema.getFullTableName() + " already exists");
    } else {
      try {
        bqRepo
            .runDDLQuery("CREATE TABLE " + datasourceSchema.getFullTableName() + " AS "
                + "SELECT *, CURRENT_DATETIME() AS load_date_time FROM " + datasourceSchema
                .getFullTableTmpName() + " LIMIT 0");
      } catch (Exception e) {
        throw new BigQueryLoaderException(
            "Cannot create table " + datasourceSchema.getFullTableTmpName(), e);
      }
    }

    // 3- Merge la table destination avec la table tmp
    try {
      bqRepo.runDDLQuery("MERGE\n"
          + "  `" + datasourceSchema.getFullTableName() + "` dest\n"
          + "USING\n"
          + "  `" + datasourceSchema.getFullTableTmpName() + "` src\n"
          + "ON\n"
          + "  " + primaryKeys(datasourceSchema) + "\n"
          + "WHEN NOT MATCHED THEN\n"
          + "  INSERT " + insertClause(datasourceSchema) + "\n"
          + "WHEN MATCHED THEN\n"
          + "  UPDATE SET " + updateClause(datasourceSchema)
      );
    } catch (Exception e) {
      throw new BigQueryLoaderException(
          "Cannot merge from " + datasourceSchema.getFullTableTmpName() + " into destination table "
              + datasourceSchema.getFullTableName(), e);
    }

    // 4- drop la table temporaire
    bqRepo.dropTable(datasourceSchema.getTmpTableId());
  }

  private String primaryKeys(DatasourceSchema datasourceSchema) {
    return datasourceSchema.getColumns().stream()
        .filter(Column::isPrimaryKey)
        .map(column -> "src." + column.getName() + " = dest." + column.getName())
        .collect(Collectors.joining(", "));
  }

  private String insertClause(DatasourceSchema datasourceSchema) {
    String columns = datasourceSchema.getColumns().stream()
        .map(Column::getName)
        .collect(Collectors.joining(", ")) + ", load_date_time";
    String values = datasourceSchema.getColumns().stream()
        .map(Column::getName)
        .collect(Collectors.joining(", ")) + ", CURRENT_DATETIME()";

    return "(" + columns + ") VALUES (" + values + ")";
  }

  private String updateClause(DatasourceSchema datasourceSchema) {
    return datasourceSchema.getColumns().stream()
        .filter(column -> !column.isPrimaryKey())
        .map(column -> column.getName() + " = src." + column.getName())
        .collect(Collectors.joining(", "));
  }
}
