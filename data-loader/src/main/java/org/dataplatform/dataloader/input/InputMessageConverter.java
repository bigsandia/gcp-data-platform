package org.dataplatform.dataloader.input;

import com.google.api.services.storage.model.Notification;
import com.google.gson.Gson;

import java.util.Base64;

public class InputMessageConverter {

    public static Notification extractNotificationMessage(String body) {
        InputMessage inputMessage = fromJson(body);
        String messageData = inputMessage.getMessage().getData();
        String notificationAsString = new String(Base64.getDecoder().decode(messageData));

        return new Gson().fromJson(notificationAsString, Notification.class);
    }

    private static InputMessage fromJson(String json) {
        return new Gson().fromJson(json, InputMessage.class);
    }
}
