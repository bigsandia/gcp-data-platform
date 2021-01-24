package org.dataplatform.dataloader.test.tools

import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions

trait CloudStorageTesting extends CommonTesting {

    String fileIsCopiedIntoBucket(file,bucketName) {
        def content = getResourceAsBytes(file)
        BlobInfo blodInfo = BlobInfo.newBuilder(bucketName, file).build();
        Storage storage = StorageOptions.newBuilder().build().getService();
        storage.create(blodInfo, content);

        return "gs://" + bucketName + "/" + file;
    }
}
