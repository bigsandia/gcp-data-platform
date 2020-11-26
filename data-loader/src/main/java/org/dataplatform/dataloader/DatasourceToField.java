package org.dataplatform.dataloader;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.dataplatform.dataloader.model.Column;


public class DatasourceToField {

  public static List<Field> buildFields(List<Column> columns, boolean addLoadDateTimeField) {
    List<Field> fields = columns.stream()
        .map(DatasourceToField::buildField)
        .collect(Collectors.toList());

    if (addLoadDateTimeField) {
      fields.add(Field.newBuilder(Column.LOAD_DATE_TIME_FIELD, LegacySQLTypeName.DATETIME)
          .setDescription("Loading date time")
          .setMode(Field.Mode.NULLABLE)
          .build());
    }

    return fields;
  }

  private static Field buildField(Column column) {
    final List<Field> subfields = new ArrayList<>();
    if (column.hasSubColumns()) {
      column.getSubColumns().forEach(c -> subfields.add(buildField(c)));
    }
    Field field;
    if (subfields.isEmpty()) {
      field = Field.newBuilder(
          column.getName(),
          toBigQueryType(adaptFieldTypeToFeatureLoading(column)))
          .setDescription(column.getDescription())
          .setMode(Field.Mode.valueOf(column.getMode().toString())).build();
    } else {
      field = Field.newBuilder(
          column.getName(),
          LegacySQLTypeName.RECORD,
          FieldList.of(subfields))
          .setDescription(column.getDescription())
          .setMode(Field.Mode.valueOf(column.getMode().toString())).build();
    }
    return field;
  }

  private static String adaptFieldTypeToFeatureLoading(Column column) {
    if (column.columnWithDateConversion() && !column.getPattern().get().equals("yyyy-MM-dd")) {
      return "STRING";
    } else {
      return column.getType().toString();
    }
  }

  private static LegacySQLTypeName toBigQueryType(String type) {
    return LegacySQLTypeName.valueOfStrict(type);
  }

}
