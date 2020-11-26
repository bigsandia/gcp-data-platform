package org.dataplatform.dataloader.loaders;


import org.dataplatform.dataloader.model.DatasourceSchema;

public interface BigQueryLoader {

  void load(String filename, DatasourceSchema datasourceSchema) throws BigQueryLoaderException;

}
