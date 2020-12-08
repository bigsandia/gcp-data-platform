package org.dataplatform.dataloader;

import static spark.Spark.port;
import static spark.Spark.post;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.input.InputMessage;
import org.dataplatform.dataloader.input.InputMessageUtils;

public class Application {

    private static final Logger LOGGER = LogManager.getLogger(Application.class);

    public static void main(String[] args) {
        if (null == System.getenv("DATA_LOCATION")) {
            LOGGER
                .warn("DATA_LOCATION env var is not set. Should be set for example to US, EU, ...");
            System.exit(1);
        }
        String configBucketName = System.getenv("CONFIG_BUCKET_NAME");
        if (null == configBucketName) {
            throw new IllegalArgumentException("Missing CONFIG_BUCKET_NAME env var");
        }

        port(8080);

        post("/", (req, res) -> {
            LOGGER.info("Req body={}", req.body());
            InputMessage inputMessage = InputMessageUtils.fromJson(req.body());
            LOGGER.info("inputMessage.inputMessage.getMessage().getData()={}",
                inputMessage.getMessage().getData());

            DataLoaderConfig config = new DataLoaderConfig(configBucketName);
            new DataLoader(config, inputMessage).run();
            return "OK";
        });
    }

}
