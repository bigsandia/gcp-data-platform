package org.dataplatform.dataloader.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

  private Column(String name, ColumnType type,
      String description, String pattern, boolean primaryKey) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.pattern = pattern;
    this.primaryKey = primaryKey;
  }

  public String getName() {
    return name;
  }

  public ColumnType getType() {
    return type;
  }

  public ColumnMode getMode() {
    return mode;
  }

  public String getDescription() {
    return description;
  }

  public Optional<String> getPattern() {
    return Optional.ofNullable(pattern);
  }

  public boolean isPrimaryKey() {
    return primaryKey;
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
