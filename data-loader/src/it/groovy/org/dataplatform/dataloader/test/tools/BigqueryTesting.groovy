package org.dataplatform.dataloader.test.tools

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.TableId

trait BigqueryTesting {

    private BigQuery bigQuery = BigQueryOptions.newBuilder()
            .build().getService()

    def getResult(query) {
        def tableResult = bigQuery.query(QueryJobConfiguration.of(query))
        def fieldNames = tableResult.getSchema().getFields().collect { it.name }
        return tableResult.iterateAll()
                .collect{ raw->
                    fieldNames.collectEntries { fieldName ->
                        [fieldName, raw.get(fieldName).getValue()]
                    }
                }
    }

    def deleteTableIfExist(TableId table) {
        bigQuery.delete(table)
    }

}
