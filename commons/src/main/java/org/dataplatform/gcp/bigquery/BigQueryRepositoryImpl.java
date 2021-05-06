package org.dataplatform.gcp.bigquery;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.bp.Duration;

public class BigQueryRepositoryImpl implements BigQueryRepository {

  private static final Logger LOGGER = LogManager.getLogger(BigQueryRepositoryImpl.class);

  private final BigQuery bigQuery;
  private final String dataLocation;

  public BigQueryRepositoryImpl(String dataLocation) {
    this.dataLocation = dataLocation;
    bigQuery = BigQueryOptions.newBuilder()
        .build().getService();
  }

  public void setTableDescription(TableId tableId, String description) {
    bigQuery.getTable(tableId)
        .toBuilder()
        .setDescription(description)
        .build()
        .update();
  }

  @Override
  public void setTableLabels(TableId tableId, Map<String, String> labels) {
    bigQuery.getTable(tableId)
        .toBuilder()
        .setLabels(labels)
        .build()
        .update();
  }

  @Override
  public void setFieldsDescription(TableId tableId, Map<String, String> descriptionByField) {
    TreeMap<String, String> descriptionByFieldCaseInsensitive = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    descriptionByFieldCaseInsensitive.putAll(descriptionByField);

    Table table = bigQuery.getTable(tableId);
    TableDefinition tableDefinition = table.getDefinition();

    List<Field> updatedFields = tableDefinition.getSchema().getFields()
        .stream()
        .map(field -> Field
            .newBuilder(field.getName().toLowerCase(), field.getType(), field.getSubFields())
            .setDescription(descriptionByFieldCaseInsensitive.getOrDefault(field.getName(), field.getDescription()))
            .setMode(field.getMode())
            .build())
        .collect(Collectors.toList());

    table.toBuilder()
        .setDefinition(tableDefinition.toBuilder().setSchema(Schema.of(updatedFields)).build())
        .build()
        .update();
  }

  public void dropTable(TableId tableId) {
    bigQuery.delete(tableId);
  }

  public void truncateTableIfExists(TableId tableId) throws InterruptedException {
    Table table = bigQuery.getTable(tableId);

    if (table != null) {
      runDDLQuery(String.format(
          "DELETE FROM %s.%s.%s WHERE true",
          tableId.getProject(),
          tableId.getDataset(),
          tableId.getTable()
      ));
    }
  }

  public boolean tableExists(TableId tableId) {
    return bigQuery.getTable(tableId) != null;
  }

  public void runDDLQuery(String query) throws InterruptedException {
    QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration
        .newBuilder(query)
        .build();
    Job job = bigQuery
        .create(JobInfo.of(queryJobConfiguration));
    job.waitFor(RetryOption.maxAttempts(3));
    LOGGER.info("Job succeeded");
  }

  public void runJob(JobConfiguration jobConfiguration, String jobNamePrefix) {
    try {
      String jobName = jobNamePrefix + "-" + UUID.randomUUID().toString();

      JobId jobId = JobId.newBuilder().setJob(jobName).setLocation(dataLocation).build();

      Job job = bigQuery.create(JobInfo.of(jobId, jobConfiguration));

      Job completedJob = job.waitFor(
          RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
          RetryOption.totalTimeout(Duration.ofMinutes(3)));
      if (completedJob != null && completedJob.getStatus().getError() == null) {
        LOGGER.info("Job {} executed successfully", jobName);
      } else {
        if (completedJob != null) {
          throw new IllegalStateException("job error : " +
              completedJob.getStatus().getError() + ", execution errors : " +
              completedJob.getStatus().getExecutionErrors()
          );
        } else {
          throw new IllegalStateException("job error jobName:" + jobName);
        }
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException("job error", e);
    }
  }
}
