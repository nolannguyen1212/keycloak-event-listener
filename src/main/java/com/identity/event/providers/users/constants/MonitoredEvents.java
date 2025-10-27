package com.identity.event.providers.users.constants;

import org.keycloak.events.EventType;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;

import java.util.Map;
import java.util.Set;

public class MonitoredEvents {
    
    private static final Set<EventType> USER_EVENTS = Set.of(
            EventType.LOGIN,
            EventType.REGISTER,
            EventType.UPDATE_PROFILE,
            EventType.UPDATE_EMAIL,
            EventType.VERIFY_EMAIL,
            EventType.RESET_PASSWORD
    );

    private static final Map<ResourceType, Set<OperationType>> ADMIN_EVENTS = Map.of(
            ResourceType.USER, Set.of(
                    OperationType.CREATE,
                    OperationType.UPDATE,
                    OperationType.DELETE
            ),
            ResourceType.REALM, Set.of(
                    OperationType.UPDATE
            ),
            ResourceType.CLIENT, Set.of(
                    OperationType.CREATE,
                    OperationType.DELETE
            )
    );
    
    public static boolean isUserEventMonitored(EventType type) {
        return USER_EVENTS.contains(type);
    }
    
    public static Set<EventType> getMonitoredUserEvents() {
        return USER_EVENTS;
    }
    
    public static boolean isAdminEventMonitored(ResourceType resource, OperationType operation) {
        Set<OperationType> operations = ADMIN_EVENTS.get(resource);
        return operations != null && operations.contains(operation);
    }
    
    public static Map<ResourceType, Set<OperationType>> getMonitoredAdminEvents() {
        return ADMIN_EVENTS;
    }
}