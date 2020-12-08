package org.dataplatform.dataloader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSDatasourceSchemasRetriever implements DatasourceSchemasRetriever {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(GCSDatasourceSchemasRetriever.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .registerModule(new JavaTimeModule());

  private final Map<Pattern, DatasourceSchema> datasources;

  public GCSDatasourceSchemasRetriever(String bucketName) {
    datasources = new HashMap<>();

    Storage storage = StorageOptions.getDefaultInstance().getService();
    Page<Blob> blobs = storage.list(bucketName);
    for (Blob blob : blobs.iterateAll()) {
      String blobContent = new String(blob.getContent(), StandardCharsets.UTF_8);
      try {
        DatasourceSchema datasourceSchema = OBJECT_MAPPER
            .readValue(blobContent, DatasourceSchema.class);
        LOGGER.info("loading datasource {}", blob.getName());
        datasources.put(Pattern.compile(datasourceSchema.getRawPath()), datasourceSchema);
      } catch (IOException e) {
        LOGGER.error("error while loading datasource {}", blob.getName());
      }
    }
  }

  @Override
  public List<DatasourceSchema> findCorrespondingSchemas(String inputFileNameOnGcs) {
    return datasources.entrySet()
        .stream()
        .filter(patternAndDatasource ->
            patternAndDatasource.getKey().matcher(inputFileNameOnGcs).find())
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }

}
