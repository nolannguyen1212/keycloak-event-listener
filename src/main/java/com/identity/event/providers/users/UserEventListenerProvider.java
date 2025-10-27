package com.identity.event.providers.users;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.identity.config.EnvConfig;
import com.identity.event.providers.users.constants.MonitoredEvents;
import com.identity.kafka.EventProducer;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;

import java.util.HashMap;
import java.util.Map;

public class UserEventListenerProvider implements EventListenerProvider {

    private static final Logger logger = Logger.getLogger(UserEventListenerProvider.class);
    private final EventProducer eventProducer;
    private final ObjectMapper objectMapper;

    public UserEventListenerProvider() {
        this.eventProducer = new EventProducer();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void onEvent(Event event) {    
        if (!EnvConfig.isRealmAllowed(event.getRealmName())) {
            logger.debugf("Realm %s not allowed. Allowed realms: %s", 
                        event.getRealmId(), EnvConfig.ALLOWED_REALMS);
            return;
        }

        if (!MonitoredEvents.isUserEventMonitored(event.getType())) {
            return;
        }

        try {
            Map<String, Object> eventData = buildUserEventData(event);
            String eventJson = objectMapper.writeValueAsString(eventData);
            String messageKey = event.getUserId() != null ? event.getUserId() : event.getRealmId();
            eventProducer.sendUserEvent(messageKey, eventJson);
        } catch (Exception e) {
            logger.errorf(e, "Failed to process user event: %s", event.getType());
        }
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        if (!EnvConfig.isRealmAllowed(event.getRealmId())) return;
        if (!MonitoredEvents.isAdminEventMonitored(event.getResourceType(), event.getOperationType())) return;

        try {
            Map<String, Object> eventData = buildAdminEventData(event, includeRepresentation);
            String eventJson = objectMapper.writeValueAsString(eventData);
            eventProducer.sendAdminEvent(event.getRealmId(), eventJson);
        } catch (Exception e) {
            logger.errorf(e, "Failed to process admin event: %s", event.getOperationType());
        }
    }

    @Override
    public void close() {
        eventProducer.close();
    }

    private Map<String, Object> buildUserEventData(Event event) {
        Map<String, Object> data = new HashMap<>();
        data.put("eventType", event.getType().toString());
        data.put("realmId", event.getRealmId());
        data.put("clientId", event.getClientId());
        data.put("userId", event.getUserId());
        data.put("sessionId", event.getSessionId());
        data.put("timestamp", event.getTime());
        data.put("ipAddress", event.getIpAddress());

        if (event.getError() != null) {
            data.put("error", event.getError());
        }

        if (event.getDetails() != null && !event.getDetails().isEmpty()) {
            data.put("details", event.getDetails());
        }

        data.put("metadata", Map.of(
            "source", "keycloak",
            "eventCategory", "user"
        ));

        return data;
    }

    private Map<String, Object> buildAdminEventData(AdminEvent event, boolean includeRepresentation) {
        Map<String, Object> data = new HashMap<>();
        data.put("operationType", event.getOperationType().toString());
        data.put("resourceType", event.getResourceType().toString());
        data.put("resourcePath", event.getResourcePath());
        data.put("realmId", event.getRealmId());
        data.put("timestamp", event.getTime());

        if (event.getAuthDetails() != null) {
            data.put("authDetails", Map.of(
                "userId", event.getAuthDetails().getUserId(),
                "ipAddress", event.getAuthDetails().getIpAddress(),
                "realmId", event.getAuthDetails().getRealmId()
            ));
        }

        if (includeRepresentation && event.getRepresentation() != null) {
            data.put("representation", event.getRepresentation());
        }

        if (event.getError() != null) {
            data.put("error", event.getError());
        }

        data.put("metadata", Map.of(
            "source", "keycloak",
            "eventCategory", "admin"
        ));

        return data;
    }
}