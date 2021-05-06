package org.dataplatform.dataloader;

import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.LoadJobConfiguration.Builder;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.loaders.DatasourceToField;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.dataplatform.gcp.bigquery.BigQueryRepository;

public class GcsFileToBqTableLoader {

  private static final Logger LOGGER = LogManager.getLogger(GcsFileToBqTableLoader.class);

  private final BigQueryRepository bqRepo;
  private final String gcsFileName;
  private final DatasourceSchema datasourceSchema;

  public GcsFileToBqTableLoader(BigQueryRepository bqRepo, String gcsFileName,
      DatasourceSchema datasourceSchema) {
    this.bqRepo = bqRepo;
    this.gcsFileName = gcsFileName;
    this.datasourceSchema = datasourceSchema;
  }

  public void load() {
    LoadJobConfiguration loadJobConfiguration = createLoadJobConfiguration();
    bqRepo.runJob(loadJobConfiguration, "data-loader");
    LOGGER.info("Successfully create table {}", loadJobConfiguration.getDestinationTable());
  }

  private LoadJobConfiguration createLoadJobConfiguration() {
    TableId tableId = datasourceSchema.getTmpTableId();

    List<Field> fields = DatasourceToField.buildFields(datasourceSchema.getColumns(), false);

    Builder builder = LoadJobConfiguration.builder(tableId, gcsFileName)
        .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
        .setSchema(Schema.of(fields));

    LoadJobConfiguration loadJobConfiguration;
    if (isJson()) {
      loadJobConfiguration = builder
          .setFormatOptions(FormatOptions.json())
          .build();
    } else {
      loadJobConfiguration = builder
          .setFormatOptions(CsvOptions.newBuilder()
              .setFieldDelimiter(datasourceSchema.getDelimiter())
              .setQuote(datasourceSchema.getQuote())
              .setAllowJaggedRows(true)
              .setAllowQuotedNewLines(datasourceSchema.isAllowQuotedNewlines())
              .setEncoding(datasourceSchema.getCharset())
              .setSkipLeadingRows(datasourceSchema.getLeadingRows()).build())
          .build();
    }

    return loadJobConfiguration;
  }

  private boolean isJson() {
    return gcsFileName.endsWith("json");
  }
}
