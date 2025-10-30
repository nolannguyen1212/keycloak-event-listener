package com.identity.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.identity.kafka.config.EnvConfig;
import com.identity.kafka.config.EventsConfig;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;

import java.util.HashMap;
import java.util.Map;

public class KafkaEventProvider implements EventListenerProvider {

    private static final Logger log = Logger.getLogger(KafkaEventProvider.class);
    private final EventProducer producer;
    private final ObjectMapper mapper;
    private final EventsConfig config;

    public KafkaEventProvider() {
        this.producer = new EventProducer();
        this.mapper = new ObjectMapper();
        this.config = EventsConfig.get();
    }

    @Override
    public void onEvent(Event event) {
        if (!EnvConfig.isRealmAllowed(event.getRealmName()))
            return;
        if (!config.isUserEventMonitored(event.getType()))
            return;

        try {
            Map<String, Object> data = new HashMap<>();
            data.put("type", event.getType().toString());
            data.put("realm", event.getRealmId());
            data.put("user", event.getUserId());
            data.put("client", event.getClientId());
            data.put("time", event.getTime());
            data.put("ip", event.getIpAddress());
            if (event.getError() != null)
                data.put("error", event.getError());
            if (event.getDetails() != null)
                data.put("details", event.getDetails());

            String json = mapper.writeValueAsString(data);
            String key = event.getUserId() != null ? event.getUserId() : event.getRealmId();
            producer.sendUser(key, json);
        } catch (Exception e) {
            log.errorf(e, "Failed user event: %s", event.getType());
        }
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRep) {
        if (!EnvConfig.isRealmAllowed(event.getRealmId()))
            return;
        if (!config.isAdminEventMonitored(event.getResourceType(), event.getOperationType()))
            return;

        try {
            Map<String, Object> data = new HashMap<>();
            data.put("operation", event.getOperationType().toString());
            data.put("resource", event.getResourceType().toString());
            data.put("path", event.getResourcePath());
            data.put("realm", event.getRealmId());
            data.put("time", event.getTime());
            if (event.getError() != null)
                data.put("error", event.getError());
            if (includeRep && event.getRepresentation() != null) {
                data.put("representation", event.getRepresentation());
            }

            String json = mapper.writeValueAsString(data);
            producer.sendAdmin(event.getRealmId(), json);
        } catch (Exception e) {
            log.errorf(e, "Failed admin event: %s", event.getOperationType());
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}