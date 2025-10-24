package org.example.event.providers.users.constants;

import org.keycloak.events.EventType;

import java.util.Set;

public class UserEventTypes {

    private static final Set<EventType> MONITORED = Set.of(
            EventType.LOGIN,
            EventType.LOGOUT
    );

    public static boolean isMonitored(EventType type) {
        return MONITORED.contains(type);
    }
}