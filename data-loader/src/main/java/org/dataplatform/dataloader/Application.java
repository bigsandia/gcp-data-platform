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


        DataLoaderConfig config = DataLoaderConfig.fromMap(System.getenv());
        GCSDatasourceSchemasRetriever datasourceSchemasRetriever = new GCSDatasourceSchemasRetriever(config.getConfigBucketName());

        port(8080);
        post("/", (req, res) -> {
            LOGGER.info("Req body={}", req.body());

            Notification notification = InputMessageConverter.extractNotificationMessage(req.body());
            LOGGER.info("notification={}", notification);
            new DataLoader(datasourceSchemasRetriever).run(notification);
            return "OK";
        });
    }

}
