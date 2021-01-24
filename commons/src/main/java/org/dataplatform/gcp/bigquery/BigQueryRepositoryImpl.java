package org.dataplatform.gcp.bigquery;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.util.UUID;
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

  public void runJob(LoadJobConfiguration jobConfiguration, String jobNamePrefix) {
    try {
      String jobName = jobNamePrefix + "-" + UUID.randomUUID().toString();

      JobId jobId = JobId.newBuilder().setJob(jobName).setLocation(dataLocation).build();

      Job job = bigQuery.create(JobInfo.of(jobId, jobConfiguration));

      Job completedJob = job.waitFor(
          RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
          RetryOption.totalTimeout(Duration.ofMinutes(3)));
      if (completedJob != null && completedJob.getStatus().getError() == null) {

        LOGGER.info("Successfully create table {}", jobConfiguration.getDestinationTable());
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
