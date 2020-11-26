package org.dataplatform.dataloader;

import java.util.List;
import org.dataplatform.dataloader.model.DatasourceSchema;

public interface DatasourceSchemasRetriever {

  List<DatasourceSchema> findCorrespondingSchemas(String inputFileNameOnGcs);
}
