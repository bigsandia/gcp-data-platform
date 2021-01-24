package org.dataplatform.dataloader.loaders


import org.dataplatform.dataloader.common.JsonParser
import org.dataplatform.dataloader.model.DatasourceSchema
import org.dataplatform.dataloader.test.tools.BigqueryTesting
import org.dataplatform.dataloader.test.tools.CloudStorageTesting
import org.dataplatform.gcp.bigquery.BigQueryRepositoryImpl
import spock.lang.Specification

class BigQueryLoaderDeltaIT extends Specification implements BigqueryTesting, CloudStorageTesting {

    static String TEST_RAW_BUCKET = "test-it-raw-bucket-a23bc"
    private BigQueryLoaderDelta bigQueryLoaderDelta

    void setup() {
        bigQueryLoaderDelta = new BigQueryLoaderDelta(new BigQueryRepositoryImpl("EU"))
    }

    def "Delta loader should create the table when not already exist and update table based on primary key"() {
        given:
        def datasourceSchema = retrieveDatasourceSchema("ingestion-case/delta/conf-delta.json")
        deleteTableIfExist(datasourceSchema.getTableId())
        when:
        bigQueryLoaderDelta.load(
                fileIsCopiedIntoBucket("ingestion-case/delta/raw-data/delta_1.csv", TEST_RAW_BUCKET),
                datasourceSchema)

        then:
        def firstIngestionResult = getResult("SELECT * FROM test_it.delta order by ID")
        firstIngestionResult.size() == 2
        verifyAll(firstIngestionResult[0]) {
            ID == "1"
            name == "Pierre"
            birthdate == "1980-01-01"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }
        verifyAll (firstIngestionResult[1]) {
            ID == "2"
            name == "Paul"
            birthdate == "1975-02-15"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }

        when:
        bigQueryLoaderDelta.load(
                fileIsCopiedIntoBucket("ingestion-case/delta/raw-data/delta_2.csv", TEST_RAW_BUCKET),
                datasourceSchema)
        then:
        def secondIngestionResult = getResult("SELECT * FROM test_it.delta order by ID")
        secondIngestionResult.size() == 3
        verifyAll(secondIngestionResult[0]) {
            ID == "1"
            name == "Pierre"
            birthdate == "1980-01-20"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }
        verifyAll(secondIngestionResult[1]) {
            ID == "2"
            name == "Paul"
            birthdate == "1975-02-15"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }
        verifyAll(secondIngestionResult[2]) {
            ID == "3"
            name == "Jacques"
            birthdate == "1970-11-27"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }
    }

    def initTableWith(file, datasourceSchema) {
        deleteTableIfExist(datasourceSchema.getTableId())
        def filePath = fileIsCopiedIntoBucket(file, TEST_RAW_BUCKET)
        bigQueryLoaderDelta.load(filePath, datasourceSchema)
    }

    private DatasourceSchema retrieveDatasourceSchema(String path) {
        byte[] content = getResourceAsBytes(path)
        return JsonParser.parseDatasourceSchema(content)
    }

}
