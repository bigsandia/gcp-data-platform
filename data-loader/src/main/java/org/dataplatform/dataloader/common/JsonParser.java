package org.dataplatform.dataloader.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.dataplatform.dataloader.model.DatasourceSchema;

public class JsonParser {

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper()
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .registerModule(new JavaTimeModule());

  public static DatasourceSchema parseDatasourceSchema(byte[] jsonAsString) {
    try {
      return OBJECT_MAPPER.readValue(jsonAsString, DatasourceSchema.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
