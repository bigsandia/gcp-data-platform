package org.dataplatform.dataloader.loaders;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.GcsFileToBqTableLoader;
import org.dataplatform.dataloader.model.Column;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.dataplatform.gcp.bigquery.BigQueryRepository;
import org.dataplatform.gcp.bigquery.BigQueryRepositoryImpl;

public class BigQueryLoaderFull implements BigQueryLoader {

  public static Logger LOGGER = LogManager.getLogger(BigQueryLoaderFull.class);

  @Override
  public void load(String filename, DatasourceSchema datasourceSchema)
      throws BigQueryLoaderException {
    BigQueryRepository bqRepo = new BigQueryRepositoryImpl();

    // 1- charger dans une table temporaire
    GcsFileToBqTableLoader gcsFileToBqTableLoader = new GcsFileToBqTableLoader(
        bqRepo, filename, datasourceSchema);
    gcsFileToBqTableLoader.load();

    // 2- delete all dans la table cible
    try {
      bqRepo.truncateTableIfExists(datasourceSchema.getTableId());
    } catch (Exception e) {
      throw new BigQueryLoaderException(
          "Cannot delete call in table " + datasourceSchema.getFullTableName(), e);
    }

    // 2.5 - crée la table si elle n'existe pas
    if (bqRepo.tableExists(datasourceSchema.getTableId())) {
      System.out.println("Table " + datasourceSchema.getFullTableName() + " already exists");
      LOGGER.info("Table {} already exists", datasourceSchema.getFullTableName());
    } else {
      try {
        bqRepo
            .runDDLQuery("CREATE TABLE " + datasourceSchema.getFullTableName() + " AS "
                + "SELECT " + columns(datasourceSchema.getColumns())
                + ", CURRENT_DATETIME() AS load_date_time FROM " + datasourceSchema
                .getFullTableTmpName() + " LIMIT 0");
      } catch (Exception e) {
        throw new BigQueryLoaderException(
            "Cannot create table " + datasourceSchema.getFullTableTmpName(), e);
      }
    }

    // 3- déverse tout la table cible avec le champ load date time
    try {
      String query = "INSERT INTO `" + datasourceSchema.getFullTableName() + "` "
          + "SELECT " + columns(datasourceSchema.getColumns())
          + ", CURRENT_DATETIME() AS load_date_time FROM `" + datasourceSchema.getFullTableTmpName()
          + "`";
      bqRepo.runDDLQuery(query);
    } catch (Exception e) {
      throw new BigQueryLoaderException(
          "Cannot insert from " + datasourceSchema.getFullTableTmpName()
              + " into destination table " + datasourceSchema.getFullTableName(), e);
    }

    // 4- drop la table temporaire
    bqRepo.dropTable(datasourceSchema.getTmpTableId());
  }

  private String columns(List<Column> columns) {
    return columns.stream()
        .map(Column::getName)
        .collect(Collectors.joining(", "));
  }

}
