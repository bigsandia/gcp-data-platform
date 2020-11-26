package org.dataplatform.dataloader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.dataplatform.dataloader.model.DatasourceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalDatasourceSchemasRetriever implements DatasourceSchemasRetriever {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(LocalDatasourceSchemasRetriever.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
      .registerModule(new JavaTimeModule());

  private final Map<Pattern, DatasourceSchema> datasources;

  public LocalDatasourceSchemasRetriever() {
    try (Stream<Path> paths = Files.walk(Paths.get(Resources.getResource("schema").toURI()))) {
      List<DatasourceSchema> datasourceSchemas = paths
          .filter(path -> path.toString().endsWith(".json"))
          .peek(path -> LOGGER.debug("loading datasource {}", path.toFile().getName()))
          .map(path -> {
            try {
              return OBJECT_MAPPER
                  .readValue(path.toFile(), DatasourceSchema.class);
            } catch (IOException e) {
              e.printStackTrace();
            }
            return null;
          })
          .collect(Collectors.toList());

      datasources = datasourceSchemas
          .stream()
          .collect(Collectors.toMap(datasource -> Pattern.compile(datasource.getRawPath()),
              datasource -> datasource));
    } catch (URISyntaxException | IOException e) {
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
