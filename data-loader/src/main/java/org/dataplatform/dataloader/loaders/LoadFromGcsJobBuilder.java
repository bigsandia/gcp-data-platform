package org.dataplatform.dataloader.loaders;

import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import java.util.List;
import org.dataplatform.dataloader.model.DatasourceSchema;

public class LoadFromGcsJobBuilder {

  private final DatasourceSchema datasourceSchema;
  private String sourceUri;
  private TableId destinationTable;

  private LoadFromGcsJobBuilder(DatasourceSchema datasourceSchema) {
    this.datasourceSchema = datasourceSchema;
  }

  public static LoadFromGcsJobBuilder createLoadJobFromSchema(DatasourceSchema datasourceSchema) {
    return new LoadFromGcsJobBuilder(datasourceSchema);
  }

  public LoadFromGcsJobBuilder withSourceUri(String sourceUri) {
    this.sourceUri = sourceUri;
    return this;
  }

  public LoadFromGcsJobBuilder withDestinationTable(TableId destinationTable) {
    this.destinationTable = destinationTable;
    return this;
  }

  public LoadJobConfiguration build() {
    List<Field> fields = DatasourceToField.buildFields(datasourceSchema.getColumns(), true);

    LoadJobConfiguration.Builder builder =
        LoadJobConfiguration.builder(destinationTable, sourceUri)
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
            .setSchema(Schema.of(fields));

    if (isJson(sourceUri)) {
      return builder.setFormatOptions(FormatOptions.json()).build();
    } else {
      return builder
          .setFormatOptions(
              CsvOptions.newBuilder()
                  .setFieldDelimiter(datasourceSchema.getDelimiter())
                  .setQuote(datasourceSchema.getQuote())
                  .setAllowJaggedRows(true)
                  .setAllowQuotedNewLines(datasourceSchema.isAllowQuotedNewlines())
                  .setEncoding(datasourceSchema.getCharset())
                  .setSkipLeadingRows(datasourceSchema.getLeadingRows())
                  .build())
          .build();
    }
  }

  private boolean isJson(String gcsFileName) {
    return gcsFileName.endsWith("json");
  }
}
