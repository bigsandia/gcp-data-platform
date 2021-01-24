package org.dataplatform.dataloader;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.dataplatform.dataloader.common.JsonParser;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSDatasourceSchemasRetriever implements DatasourceSchemasRetriever {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(GCSDatasourceSchemasRetriever.class);

  private final Map<Pattern, DatasourceSchema> datasources;

  public GCSDatasourceSchemasRetriever(String bucketName) {
    datasources = new HashMap<>();

    Storage storage = StorageOptions.getDefaultInstance().getService();
    Page<Blob> blobs = storage.list(bucketName);
    for (Blob blob : blobs.iterateAll()) {
      try {
        DatasourceSchema datasourceSchema = JsonParser.parseDatasourceSchema(blob.getContent());
        LOGGER.info("loading datasource {}", blob.getName());
        datasources.put(Pattern.compile(datasourceSchema.getRawPath()), datasourceSchema);
      } catch (RuntimeException e) {
        LOGGER.error("error while loading datasource {}", blob.getName());
      }
    }
  }

  @Override
  public List<DatasourceSchema> findCorrespondingSchemas(String inputFileNameOnGcs) {
    LOGGER.debug("Looking for DatasourceSchema for {}", inputFileNameOnGcs);
    List<DatasourceSchema> datasourceSchemaList = null;
    try {
      datasourceSchemaList = datasources.entrySet()
          .stream()
          .filter(patternAndDatasource ->
              patternAndDatasource.getKey().matcher(inputFileNameOnGcs).find())
          .map(Entry::getValue)
          .collect(Collectors.toList());
      LOGGER.debug("Found {} DatasourceSchema for {}", datasourceSchemaList.size(), inputFileNameOnGcs);
    } catch (Exception e) {
      LOGGER.error("Error when looking for DatasourceSchema for " + inputFileNameOnGcs, e);
      return List.of();
    }
    return datasourceSchemaList;
  }

}
