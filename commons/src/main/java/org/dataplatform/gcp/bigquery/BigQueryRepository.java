package org.dataplatform.gcp.bigquery;

import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;

public interface BigQueryRepository {

  void dropTable(TableId tableId);

  void truncateTableIfExists(TableId tableId) throws InterruptedException;

  boolean tableExists(TableId tableId);

  void runDDLQuery(String query) throws InterruptedException;

  void runJob(LoadJobConfiguration jobConfiguration, String jobNamePrefix);
}
