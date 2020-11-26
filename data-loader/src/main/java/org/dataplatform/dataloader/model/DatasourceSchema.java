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

  public HashMap<String, String> getLabels() {
    return labels;
  }

  public void setLabels(HashMap<String, String> labels) {
    this.labels = labels;
  }

  public String getRawPath() {
    return rawPath;
  }

  public void setRawPath(String rawPath) {
    this.rawPath = rawPath;
  }

  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public int getLeadingRows() {
    return leadingRows;
  }

  public void setLeadingRows(int leadingRows) {
    this.leadingRows = leadingRows;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  public String getCharset() {
    return charset;
  }

  public void setCharset(String charset) {
    this.charset = charset;
  }

  public IngestionType getIngestionType() {
    return ingestionType;
  }

  public void setIngestionType(IngestionType ingestionType) {
    this.ingestionType = ingestionType;
  }

  public String getQuote() {
    return quote;
  }

  public void setQuote(String quote) {
    this.quote = quote;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public void setColumns(List<Column> columns) {
    this.columns = columns;
  }

  public boolean isAllowQuotedNewlines() {
    return allowQuotedNewlines;
  }

  public void setAllowQuotedNewlines(boolean allowQuotedNewlines) {
    this.allowQuotedNewlines = allowQuotedNewlines;
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
