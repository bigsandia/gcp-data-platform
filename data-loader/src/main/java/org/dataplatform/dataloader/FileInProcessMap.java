package org.dataplatform.dataloader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.dataplatform.dataloader.model.DatasourceSchema;

public class FileInProcessMap {

  private static ThreadLocal<Map<String, String>> currentlyInProcess = ThreadLocal.withInitial(() -> new HashMap<>());

  public static boolean isInProcess(List<DatasourceSchema> datasourceSchemas) {
    for (DatasourceSchema schema : datasourceSchemas) {
      if (currentlyInProcess.get().containsKey(schema.getRawPath())) {
        return true;
      }
    }
    return false;
  }

  public static String getFileNameInProcess(String rawPath) {
    return currentlyInProcess.get().get(rawPath);
  }

  public static synchronized void addInProcess(String filename, List<DatasourceSchema> datasourceSchemas) {
    for (DatasourceSchema datasourceSchema : datasourceSchemas) {
      currentlyInProcess.get().put(datasourceSchema.getRawPath(), filename);
    }
  }

  public static synchronized void removeInProcess(List<DatasourceSchema> datasourceSchemas) {
    for (DatasourceSchema datasourceSchema : datasourceSchemas) {
      currentlyInProcess.get().remove(datasourceSchema.getRawPath());
    }
  }
}
