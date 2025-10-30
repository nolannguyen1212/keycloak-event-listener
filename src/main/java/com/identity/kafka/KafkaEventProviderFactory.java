package com.identity.kafka;

import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

public class KafkaEventProviderFactory implements EventListenerProviderFactory {

    private static final Logger log = Logger.getLogger(KafkaEventProviderFactory.class);

    @Override
    public KafkaEventProvider create(KeycloakSession session) {
        return new KafkaEventProvider();
    }

    @Override
    public void init(Config.Scope config) {
        log.info("Kafka event listener initializing");
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        log.info("Kafka event listener ready");
    }

    @Override
    public void close() {
        log.info("Kafka event listener closing");
        EventProducer.shutdown();
    }

    @Override
    public String getId() {
        return "ext-event-kafka";
    }
}