package org.dataplatform.dataloader.input;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class InputMessageUtils {

  public static InputMessage fromJson(String json) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    return objectMapper.readValue(json, InputMessage.class);
  }
}
