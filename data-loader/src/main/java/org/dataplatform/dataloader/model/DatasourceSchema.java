package org.dataplatform.dataloader.model;

import com.google.cloud.bigquery.TableId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class DatasourceSchema {

  private HashMap<String, String> labels;
  private String rawPath;
  private String project;
  private String dataset;
  private String table;
  private String description;
  private int leadingRows;
  private String delimiter;
  private String charset;
  private IngestionType ingestionType;
  private String quote;
  private List<Column> columns = new ArrayList<>();
  private boolean allowQuotedNewlines;

  public String getRawPath() {
    return rawPath;
  }

  public int getLeadingRows() {
    return leadingRows;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public String getCharset() {
    return charset;
  }

  public IngestionType getIngestionType() {
    return ingestionType;
  }

  public String getQuote() {
    return quote;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public boolean isAllowQuotedNewlines() {
    return allowQuotedNewlines;
  }

  public String getFullTableName() {
    return String.format("%s.%s.%s",
        this.project,
        this.dataset,
        this.table);
  }

  public String getFullTableTmpName() {
    return getFullTableName() + "_tmp";
  }

  public TableId getTableId() {
    return TableId.of(
        this.project,
        this.dataset,
        this.table);
  }

  public TableId getTmpTableId() {
    return TableId.of(
        this.project,
        this.dataset,
        this.table + "_tmp");
  }


}
