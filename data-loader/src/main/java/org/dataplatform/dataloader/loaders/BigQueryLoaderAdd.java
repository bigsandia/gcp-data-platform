package org.dataplatform.dataloader.loaders;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.GcsFileToBqTableLoader;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.dataplatform.gcp.bigquery.BigQueryRepository;
import org.dataplatform.gcp.bigquery.BigQueryRepositoryImpl;

public class BigQueryLoaderAdd implements BigQueryLoader {

  public static Logger LOGGER = LogManager.getLogger(BigQueryLoaderAdd.class);

  private final BigQueryRepository bigQueryRepository;

  public BigQueryLoaderAdd(BigQueryRepository bigQueryRepository) {
    this.bigQueryRepository = bigQueryRepository;
  }

  @Override
  public void load(String filename, DatasourceSchema datasourceSchema)
      throws BigQueryLoaderException {

    // 1- charger dans une table temporaire
    GcsFileToBqTableLoader gcsFileToBqTableLoader = new GcsFileToBqTableLoader(
        bigQueryRepository, filename, datasourceSchema);
    gcsFileToBqTableLoader.load();

    // 2- crée la table si elle n'existe pas
    if (bigQueryRepository.tableExists(datasourceSchema.getTableId())) {
      LOGGER.info("Table " + datasourceSchema.getFullTableName() + " already exists");
    } else {
      try {
        bigQueryRepository
            .runDDLQuery("CREATE TABLE " + datasourceSchema.getFullTableName() + " AS "
                + "SELECT *, CURRENT_DATETIME() AS load_date_time FROM " + datasourceSchema
                .getFullTableTmpName() + " LIMIT 0");
      } catch (Exception e) {
        throw new BigQueryLoaderException(
            "Cannot create table " + datasourceSchema.getFullTableTmpName(), e);
      }
    }

    // 3- déverse tout la table cible avec le champ load date time
    try {
      bigQueryRepository.runDDLQuery("INSERT INTO `" + datasourceSchema.getFullTableName() + "` "
          + "SELECT *, CURRENT_DATETIME() AS load_date_time FROM `" + datasourceSchema
          .getFullTableTmpName() + "`");
    } catch (Exception e) {
      throw new BigQueryLoaderException(
          "Cannot insert from " + datasourceSchema.getFullTableTmpName()
              + " into destination table " + datasourceSchema.getFullTableName(), e);
    }

    // 4- drop la table temporaire
    bigQueryRepository.dropTable(datasourceSchema.getTmpTableId());
  }
}
