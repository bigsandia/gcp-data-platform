package org.dataplatform.dataloader.model;

import java.util.ArrayList;
import java.util.List;

public class Column {

  public static final String LOAD_DATE_TIME_FIELD = "load_date_time";

  private String name;
  private ColumnType type;
  private ColumnMode mode = ColumnMode.NULLABLE;
  private String description;
  private String pattern;
  private boolean primaryKey;
  private boolean historizedColumn;
  private boolean referenceDate;
  private List<Column> subColumns;

  public Column() {}

  public Column(
      String name, ColumnType type, String description, String pattern, boolean primaryKey) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.pattern = pattern;
    this.primaryKey = primaryKey;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ColumnType getType() {
    return type;
  }

  public void setType(ColumnType type) {
    this.type = type;
  }

  public ColumnMode getMode() {
    return mode;
  }

  public void setMode(ColumnMode mode) {
    this.mode = mode;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  public boolean isPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(boolean primaryKey) {
    this.primaryKey = primaryKey;
  }

  public boolean isHistorizedColumn() {
    return historizedColumn;
  }

  public void setHistorizedColumn(boolean historizedColumn) {
    this.historizedColumn = historizedColumn;
  }

  public boolean isReferenceDate() {
    return referenceDate;
  }

  public void setReferenceDate(boolean referenceDate) {
    this.referenceDate = referenceDate;
  }

  public boolean hasSubColumns() {
    return subColumns != null;
  }

  public List<Column> getSubColumns() {
    if (subColumns == null) {
      subColumns = new ArrayList<>();
    }
    return subColumns;
  }

  public boolean columnWithDateConversion() {
    return pattern != null && !pattern.isEmpty() && type.equals(ColumnType.DATE);
  }
}
