package org.dataplatform.gcp.bigquery;

import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.TableId;
import java.util.Map;

public interface BigQueryRepository {

  void setTableDescription(TableId tableId, String description);

  void setTableLabels(TableId tableId, Map<String, String> labels);

  void setFieldsDescription(TableId tableId, Map<String, String> descriptionByField);

  void dropTable(TableId tableId);

  void truncateTableIfExists(TableId tableId) throws InterruptedException;

  boolean tableExists(TableId tableId);

  void runDDLQuery(String query) throws InterruptedException;

  void runJob(JobConfiguration jobConfiguration, String jobNamePrefix);
}
