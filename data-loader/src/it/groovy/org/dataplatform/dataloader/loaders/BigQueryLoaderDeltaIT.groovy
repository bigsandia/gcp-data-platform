package org.dataplatform.dataloader.loaders

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageClass
import com.google.cloud.storage.StorageOptions
import org.dataplatform.dataloader.common.JsonParser
import org.dataplatform.dataloader.model.DatasourceSchema
import org.dataplatform.dataloader.test.tools.BigqueryTesting
import org.dataplatform.dataloader.test.tools.CloudStorageTesting
import org.dataplatform.gcp.bigquery.BigQueryRepositoryImpl
import spock.lang.Specification

class BigQueryLoaderDeltaIT extends Specification implements BigqueryTesting, CloudStorageTesting {

    static String TEST_RAW_BUCKET = UUID.randomUUID().toString()
    static BigQueryLoaderDelta bigQueryLoaderDelta

    void createBucket() {
        Storage storage = StorageOptions.newBuilder()
                .setProjectId(System.getenv("GOOGLE_CLOUD_PROJECT"))
                .build().getService();
        storage.create(
                BucketInfo.newBuilder(TEST_RAW_BUCKET)
                        .setStorageClass(StorageClass.STANDARD)
                        .setLocation("EU")
                        .build());
    }

    void deleteBucket() {
        Storage storage = StorageOptions.newBuilder().setProjectId(System.getenv("GOOGLE_CLOUD_PROJECT")).build().getService();
        storage.list(TEST_RAW_BUCKET).iterateAll().forEach { Blob blob ->
            blob.delete()
        }
        storage.get(TEST_RAW_BUCKET).delete()
    }

    void setupSpec() {
        createBucket()
        bigQueryLoaderDelta = new BigQueryLoaderDelta(new BigQueryRepositoryImpl("EU"))
    }

    void cleanupSpec() {
        deleteBucket()
    }

    def "Delta loader should create the table when not already exist and update table based on primary key"() {
        given:
        def datasourceSchema = retrieveDatasourceSchema("ingestion-case/delta-basic/conf.json")
        createDatasetIfNoExists(datasourceSchema.getDataset())
        deleteTableIfExist(datasourceSchema.getTableId())

        when:
        bigQueryLoaderDelta.load(
                fileIsCopiedIntoBucket("ingestion-case/delta-basic/raw-data/delta_1.csv", TEST_RAW_BUCKET),
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
                fileIsCopiedIntoBucket("ingestion-case/delta-basic/raw-data/delta_2.csv", TEST_RAW_BUCKET),
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

    def "Delta loader should create and update table with a compound pk using a date with default date pattern"() {
        given:
        def datasourceSchema = retrieveDatasourceSchema("ingestion-case/delta-basic-date-as-pk/conf.json")
        createDatasetIfNoExists(datasourceSchema.getDataset())
        deleteTableIfExist(datasourceSchema.getTableId())

        when:
        bigQueryLoaderDelta.load(
                fileIsCopiedIntoBucket("ingestion-case/delta-basic-date-as-pk/raw-data/delta_1.csv", TEST_RAW_BUCKET),
                datasourceSchema)

        then:
        def firstIngestionResult = getResult("SELECT * FROM test_it.delta_basic_date_as_pk order by product_id")
        firstIngestionResult.size() == 2
        verifyAll(firstIngestionResult[0]) {
            id_date == "2021-04-29"
            product_id == "1234"
            quantity == "4"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }
        verifyAll (firstIngestionResult[1]) {
            id_date == "2021-04-29"
            product_id == "2345"
            quantity == "2342"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }

        when:
        bigQueryLoaderDelta.load(
                fileIsCopiedIntoBucket("ingestion-case/delta-basic-date-as-pk/raw-data/delta_2.csv", TEST_RAW_BUCKET),
                datasourceSchema)
        then:
        def secondIngestionResult = getResult("SELECT * FROM test_it.delta_basic_date_as_pk order by product_id")
        secondIngestionResult.size() == 3
        verifyAll(secondIngestionResult[0]) {
            id_date == "2021-04-29"
            product_id == "1234"
            quantity == "5"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }
        verifyAll(secondIngestionResult[1]) {
            id_date == "2021-04-29"
            product_id == "2345"
            quantity == "2342"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }
        verifyAll(secondIngestionResult[2]) {
            id_date == "2021-04-29"
            product_id == "2594"
            quantity == "2342"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }
    }

    def "Delta loader should create and update table with a compound pk using a date with specified pattern"() {
        given:
        def datasourceSchema = retrieveDatasourceSchema("ingestion-case/delta-date-with-pattern-as-pk/conf.json")
        createDatasetIfNoExists(datasourceSchema.getDataset())
        deleteTableIfExist(datasourceSchema.getTableId())

        when:
        bigQueryLoaderDelta.load(
                fileIsCopiedIntoBucket("ingestion-case/delta-date-with-pattern-as-pk/raw-data/delta_1.csv", TEST_RAW_BUCKET),
                datasourceSchema)

        then:
        def firstIngestionResult = getResult("SELECT * FROM test_it.delta_date_with_pattern_as_pk order by product_id")
        firstIngestionResult.size() == 2
        verifyAll(firstIngestionResult[0]) {
            id_date == "2021-04-29"
            product_id == "1234"
            quantity == "4"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }
        verifyAll (firstIngestionResult[1]) {
            id_date == "2021-04-29"
            product_id == "2345"
            quantity == "2342"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }

        when:
        bigQueryLoaderDelta.load(
                fileIsCopiedIntoBucket("ingestion-case/delta-date-with-pattern-as-pk/raw-data/delta_2.csv", TEST_RAW_BUCKET),
                datasourceSchema)
        then:
        def secondIngestionResult = getResult("SELECT * FROM test_it.delta_date_with_pattern_as_pk order by product_id")
        secondIngestionResult.size() == 3
        verifyAll(secondIngestionResult[0]) {
            id_date == "2021-04-29"
            product_id == "1234"
            quantity == "5"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }
        verifyAll(secondIngestionResult[1]) {
            id_date == "2021-04-29"
            product_id == "2345"
            quantity == "2342"
            load_date_time ==~ "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{6}"

        }
        verifyAll(secondIngestionResult[2]) {
            id_date == "2021-04-29"
            product_id == "2594"
            quantity == "2342"
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
