package org.example.event.providers.users;

import org.example.kafka.KafkaManager;
import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import java.util.Set;

public class UserEventListenerFactory implements EventListenerProviderFactory {

    private static final Logger log = Logger.getLogger(UserEventListenerFactory.class);
    private String topic;
    private Set<String> realms;

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        return new UserEventListenerProvider(KafkaManager.getProducer(), topic, realms);
    }

    @Override
    public void init(Config.Scope config) {
        topic = env("KAFKA_USER_EVENTS_TOPIC");
        String r = env("KAFKA_ALLOWED_REALMS");
        realms = r.isBlank() ? Set.of() : Set.of(r.split(","));
        log.infof("User events initialized - Topic: %s, Realms: %s", topic, realms.isEmpty() ? "ALL" : realms);
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
    }

    @Override
    public void close() {
        KafkaManager.close();
    }

    @Override
    public String getId() {
        return "user-event-listener";
    }

    private String env(String key) {
        return System.getenv(key);
    }
}