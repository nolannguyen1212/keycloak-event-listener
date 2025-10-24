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
import java.util.Set;
import java.util.regex.Pattern;

public class UserEventListenerProvider implements EventListenerProvider {

    private static final Logger log = Logger.getLogger(UserEventListenerProvider.class);
    private static final Pattern USER_ID = Pattern.compile("/users/([^/]+)");
    private static final ObjectMapper JSON = new ObjectMapper();

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Set<String> realms;

    public UserEventListenerProvider(KafkaProducer<String, String> producer, String topic, Set<String> realms) {
        this.producer = producer;
        this.topic = topic;
        this.realms = realms;
    }

    @Override
    public void onEvent(Event event) {
        if (allowed(event.getRealmId()) && UserEventTypes.isMonitored(event.getType())) {
            send(userEvent(event), event.getUserId());
        }
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        if (allowed(event.getRealmId()) && AdminEventTypes.isMonitored(event.getResourceType(), event.getOperationType())) {
            String userId = extractUserId(event.getResourcePath());
            if (userId != null) send(adminEvent(event, userId), userId);
        }
    }

    private boolean allowed(String realm) {
        return realms.isEmpty() || realms.contains(realm);
    }

    private Map<String, Object> userEvent(Event e) {
        Map<String, Object> data = new HashMap<>();
        data.put("eventType", e.getType().name());
        data.put("userId", e.getUserId());
        data.put("username", e.getDetails() != null ? e.getDetails().get("username") : null);
        data.put("realmId", e.getRealmId());
        data.put("ipAddress", e.getIpAddress());
        data.put("timestamp", e.getTime());
        return data;
    }

    private Map<String, Object> adminEvent(AdminEvent e, String userId) {
        Map<String, Object> data = new HashMap<>();
        data.put("eventType", "USER_" + e.getOperationType().name());
        data.put("userId", userId);
        data.put("realmId", e.getRealmId());
        data.put("adminUserId", e.getAuthDetails() != null ? e.getAuthDetails().getUserId() : null);
        data.put("timestamp", e.getTime());
        return data;
    }

    private void send(Map<String, Object> data, String key) {
        try {
            producer.send(new ProducerRecord<>(topic, key, JSON.writeValueAsString(data)), (meta, ex) -> {
                if (ex != null) log.errorf(ex, "Send failed");
            });
        } catch (Exception e) {
            log.errorf(e, "Serialize failed");
        }
    }

    private String extractUserId(String path) {
        if (path == null) return null;
        var m = USER_ID.matcher(path);
        return m.find() ? m.group(1) : null;
    }

    @Override
    public void close() {
    }
}