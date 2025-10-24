package org.example.event.providers.users;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.event.providers.users.constants.AdminEventTypes;
import org.example.event.providers.users.constants.UserEventTypes;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserEventListenerProvider implements EventListenerProvider {

    private static final Logger log = Logger.getLogger(UserEventListenerProvider.class);
    private static final Pattern USER_ID_PATTERN = Pattern.compile("/users/([^/]+)");

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final String topic;

    public UserEventListenerProvider(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.mapper = new ObjectMapper();
        this.topic = topic;
    }

    @Override
    public void onEvent(Event event) {
        if (UserEventTypes.isMonitored(event.getType())) {
            sendToKafka(buildUserEvent(event), event.getUserId());
        }
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        if (AdminEventTypes.isMonitored(event.getResourceType(), event.getOperationType())) {
            String userId = extractUserId(event.getResourcePath());
            if (userId != null) {
                sendToKafka(buildAdminEvent(event, userId), userId);
            }
        }
    }

    private Map<String, Object> buildUserEvent(Event event) {
        Map<String, Object> data = new HashMap<>();
        data.put("eventType", event.getType().name());
        data.put("userId", event.getUserId());
        data.put("username", event.getDetails() != null ? event.getDetails().get("username") : null);
        data.put("realmId", event.getRealmId());
        data.put("ipAddress", event.getIpAddress());
        data.put("timestamp", event.getTime());
        return data;
    }

    private Map<String, Object> buildAdminEvent(AdminEvent event, String userId) {
        Map<String, Object> data = new HashMap<>();
        data.put("eventType", "USER_" + event.getOperationType().name());
        data.put("userId", userId);
        data.put("realmId", event.getRealmId());
        data.put("adminUserId", event.getAuthDetails() != null ? event.getAuthDetails().getUserId() : null);
        data.put("timestamp", event.getTime());
        return data;
    }

    private void sendToKafka(Map<String, Object> data, String key) {
        try {
            String json = mapper.writeValueAsString(data);
            producer.send(new ProducerRecord<>(topic, key, json), (metadata, exception) -> {
                if (exception != null) {
                    log.errorf(exception, "Failed to send event to Kafka");
                } else {
                    log.debugf("Event sent successfully: %s", data.get("eventType"));
                }
            });
        } catch (Exception e) {
            log.errorf(e, "Failed to serialize event");
        }
    }

    private String extractUserId(String resourcePath) {
        if (resourcePath == null) {
            return null;
        }

        Matcher matcher = USER_ID_PATTERN.matcher(resourcePath);
        return matcher.find() ? matcher.group(1) : null;
    }

    @Override
    public void close() {

    }
}