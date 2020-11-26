package org.dataplatform.dataloader.model;

import java.time.LocalDateTime;

public class DatasourceEvent {

  private boolean success;
  private LocalDateTime date;
  private String project;
  private String dataset;
  private String table;
  private String source;
  private String message;

  public DatasourceEvent() {
  }

  public DatasourceEvent(boolean success, String project, String dataset, String table,
      String source, String message) {
    this(success, project, dataset, table, source, LocalDateTime.now(), message);
  }

  public DatasourceEvent(boolean success, String project, String dataset, String table,
      String source, LocalDateTime date, String message) {
    this.success = success;
    this.project = project;
    this.dataset = dataset;
    this.table = table;
    this.source = source;
    this.date = date;
    this.message = message;
  }
}
