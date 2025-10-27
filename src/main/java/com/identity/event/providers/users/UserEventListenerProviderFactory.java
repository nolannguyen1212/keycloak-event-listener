package com.identity.event.providers.users;

import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import com.identity.kafka.EventProducer;

public class UserEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final Logger logger = Logger.getLogger(UserEventListenerProviderFactory.class);
    private static final String PROVIDER_ID = "user-event-listener";

    @Override
    public UserEventListenerProvider create(KeycloakSession session) {
        return new UserEventListenerProvider();
    }

    @Override
    public void init(Config.Scope config) {
        logger.info("Initializing Kafka User Event Listener...");
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        logger.info("Post-initialization of Kafka User Event Listener completed");
    }

    @Override
    public void close() {
        logger.info("Closing Kafka User Event Listener");
        EventProducer.shutdown();
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }
}