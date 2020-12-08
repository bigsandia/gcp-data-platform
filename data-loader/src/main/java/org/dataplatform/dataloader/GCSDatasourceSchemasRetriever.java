package org.dataplatform.dataloader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
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

    try (FileSystem fs = CloudStorageFileSystem.forBucket(bucketName)) {
      for (Path path : Files.newDirectoryStream(fs.getPath("/"))) {
        try {
          DatasourceSchema datasourceSchema = OBJECT_MAPPER
              .readValue(path.toFile(), DatasourceSchema.class);
          LOGGER.info("loading datasource {}", path.toFile().getName());
          datasources.put(Pattern.compile(datasourceSchema.getRawPath()), datasourceSchema);
        } catch (IOException e) {
          LOGGER.error("error while loading datasource {}", path.toFile().getName());
        }
      }
    } catch (IOException e) {
      LOGGER.error("Error while reading from bucket {}", bucketName, e);
      throw new IllegalStateException(e);
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
