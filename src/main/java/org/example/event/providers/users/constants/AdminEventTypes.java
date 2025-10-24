package org.example.event.providers.users.constants;

import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;

import java.util.Map;
import java.util.Set;

public class AdminEventTypes {

    private static final Map<ResourceType, Set<OperationType>> MONITORED = Map.of(
            ResourceType.USER, Set.of(
                    OperationType.UPDATE,
                    OperationType.DELETE
            )
    );

    public static boolean isMonitored(ResourceType resource, OperationType operation) {
        Set<OperationType> operations = MONITORED.get(resource);
        return operations != null && operations.contains(operation);
    }
}