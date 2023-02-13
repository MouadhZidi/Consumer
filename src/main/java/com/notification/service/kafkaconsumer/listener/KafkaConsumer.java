package com.notification.service.kafkaconsumer.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.service.kafkaconsumer.exception.MapperException;
import com.notification.service.kafkaconsumer.model.Notification;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaConsumer {

    private static final ObjectMapper mapper = new ObjectMapper();
    @Autowired
    SimpMessagingTemplate template;

    @KafkaListener(topics = "arabe", groupId = "notification-group-id", containerFactory = "kakfaListenerContainerFactory")

    public void listenSenderEmail(String data) {

        Notification notification = fromJson(data, Notification.class);
        
        template.convertAndSend("/topic/notif", notification);
        System.out.println("cc"+notification);

    }

    /**
     * Convert json to Object
     * @param json String json value
     * @param clazz Instances of the class
     * @param <T> Object Class
     * @return Object class
     */
    private <T> T fromJson(String json, Class<T> clazz) {
        try {
            System.out.println(clazz);

            return mapper.readValue(json, clazz);
        } catch (Exception e) {
            throw new MapperException(e.getMessage());
        }
    }
}
