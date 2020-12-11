package org.dataplatform.dataloader;

import com.google.api.services.storage.model.Notification;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dataplatform.dataloader.input.InputMessageConverter;

import static spark.Spark.port;
import static spark.Spark.post;

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
        DataLoaderConfig config = new DataLoaderConfig(configBucketName);
        GCSDatasourceSchemasRetriever datasourceSchemasRetriever = new GCSDatasourceSchemasRetriever(config.getConfigBucketName());

        post("/", (req, res) -> {
            LOGGER.info("Req body={}", req.body());

            Notification notification = InputMessageConverter.extractNotificationMessage(req.body());
            LOGGER.info("notification={}", notification);
            new DataLoader(datasourceSchemasRetriever).run(notification);
            return "OK";
        });
    }

}
