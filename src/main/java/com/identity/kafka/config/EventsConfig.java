package com.identity.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;

import java.io.InputStream;
import java.util.*;

public class EventsConfig {

    private static final Logger log = Logger.getLogger(EventsConfig.class);
    private static EventsConfig instance;
    private Set<EventType> userEvents = new HashSet<>();
    private Map<ResourceType, Set<OperationType>> adminEvents = new HashMap<>();

    public static EventsConfig get() {
        if (instance == null) {
            instance = new EventsConfig();
            instance.load();
        }
        return instance;
    }

    private void load() {
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream(EnvConfig.CONFIG_FILE);
            if (is == null) {
                log.warn("Config file not found, using defaults");
                loadDefaults();
                return;
            }

            Config config = new ObjectMapper().readValue(is, Config.class);

            config.userEvents.forEach(e -> {
                try {
                    userEvents.add(EventType.valueOf(e.toUpperCase()));
                } catch (Exception ex) {
                    log.warnf("Invalid user event: %s", e);
                }
            });

            config.adminEvents.forEach((resource, ops) -> {
                try {
                    ResourceType rt = ResourceType.valueOf(resource.toUpperCase());
                    Set<OperationType> opSet = new HashSet<>();
                    ops.forEach(op -> {
                        try {
                            opSet.add(OperationType.valueOf(op.toUpperCase()));
                        } catch (Exception ex) {
                        }
                    });
                    adminEvents.put(rt, opSet);
                } catch (Exception ex) {
                }
            });

            log.infof("Loaded: %d user events, %d admin resources", userEvents.size(), adminEvents.size());
        } catch (Exception e) {
            log.error("Failed to load config", e);
            loadDefaults();
        }
    }

    private void loadDefaults() {
        userEvents.addAll(Set.of(EventType.LOGIN, EventType.REGISTER, EventType.LOGOUT));
        adminEvents.put(ResourceType.USER, Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE));
    }

    public boolean isUserEventMonitored(EventType type) {
        return userEvents.contains(type);
    }

    public boolean isAdminEventMonitored(ResourceType resource, OperationType operation) {
        Set<OperationType> ops = adminEvents.get(resource);
        return ops != null && ops.contains(operation);
    }

    static class Config {
        public List<String> userEvents = new ArrayList<>();
        public Map<String, List<String>> adminEvents = new HashMap<>();
    }
}