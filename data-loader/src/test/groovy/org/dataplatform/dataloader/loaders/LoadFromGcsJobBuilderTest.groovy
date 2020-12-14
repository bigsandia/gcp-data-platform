package org.dataplatform.dataloader.loaders

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.LegacySQLTypeName
import com.google.cloud.bigquery.TableId
import org.dataplatform.dataloader.model.Column
import org.dataplatform.dataloader.model.ColumnType
import org.dataplatform.dataloader.model.DatasourceSchema
import spock.lang.Specification

class LoadFromGcsJobBuilderTest extends Specification {

    def "should build Bigquery load job for csv input"() {
        given:
        DatasourceSchema datasourceSchema = new DatasourceSchema()
        datasourceSchema.setAllowQuotedNewlines(true)
        datasourceSchema.setDelimiter(";")
        datasourceSchema.setQuote("'")
        datasourceSchema.setCharset("UTF-8")
        datasourceSchema.setLeadingRows(2)
        datasourceSchema.setColumns(List.of(new Column("id", ColumnType.INTEGER, "the pk", "", true)))
        TableId destination = TableId.of("my-project", "my-dataset", "my-table")

        when:
        def jobConfiguration = LoadFromGcsJobBuilder.createLoadJobFromSchema(datasourceSchema)
                .withDestinationTable(destination)
                .withSourceUri("gs://aaa/test.csv")
                .build()



        then:
        def field = jobConfiguration.getSchema().getFields().get(0)
        field.getName() == "id"
        field.getType() == LegacySQLTypeName.INTEGER

        jobConfiguration.getCreateDisposition() == JobInfo.CreateDisposition.CREATE_IF_NEEDED
        jobConfiguration.getWriteDisposition() == JobInfo.WriteDisposition.WRITE_TRUNCATE
        jobConfiguration.getCsvOptions().allowJaggedRows()
        jobConfiguration.getCsvOptions().allowQuotedNewLines()
        jobConfiguration.getCsvOptions().getFieldDelimiter() == ";"
        jobConfiguration.getCsvOptions().getQuote() == "'"
        jobConfiguration.getCsvOptions().getEncoding() == "UTF-8"
        jobConfiguration.getCsvOptions().getSkipLeadingRows() == 2
    }

    def "should build Bigquery load job for json input"() {
        given:
        DatasourceSchema datasourceSchema = new DatasourceSchema()

        TableId destination = TableId.of("my-project", "my-dataset", "my-table")

        when:
        def jobConfiguration = LoadFromGcsJobBuilder.createLoadJobFromSchema(datasourceSchema)
                .withDestinationTable(destination)
                .withSourceUri("gs://aaa/test.json")
                .build()


        then:
        jobConfiguration.getCreateDisposition() == JobInfo.CreateDisposition.CREATE_IF_NEEDED
        jobConfiguration.getFormat() == "NEWLINE_DELIMITED_JSON"

    }
}
