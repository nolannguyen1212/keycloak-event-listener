package org.example.event.providers;

import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakSession;
import org.jboss.logging.Logger;

public class CustomEventListenerProvider implements EventListenerProvider {
    private static final Logger logger = Logger.getLogger(CustomEventListenerProvider.class);

    KeycloakSession keycloakSession;

    public CustomEventListenerProvider(KeycloakSession keycloakSession) {
        this.keycloakSession = keycloakSession;
    }

    @Override
    public void onEvent(Event event) {
        logger.info(event.getDetails().toString());
    }

    @Override
    public void onEvent(AdminEvent adminEvent, boolean b) {
        if (adminEvent.getOperationType().name().equals("UPDATE")) {
            logger.info(adminEvent.getRepresentation());
        }
    }

    @Override
    public void close() {

    }
}
