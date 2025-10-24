package org.example.event.providers.users;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.event.providers.users.constants.KafkaTopics;
import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import java.util.Properties;

public class UserEventListenerFactory implements EventListenerProviderFactory {

    private static final Logger log = Logger.getLogger(UserEventListenerFactory.class);
    private KafkaProducer<String, String> producer;
    private String topic;

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        return new UserEventListenerProvider(producer, topic);
    }

    @Override
    public void init(Config.Scope config) {
        String bootstrapServers = config.get("bootstrapServers", "kafka:9092");
        topic = config.get("topic", KafkaTopics.USER_EVENTS);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "keycloak-user-events");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        producer = new KafkaProducer<>(props);

        log.infof("User Events Kafka initialized - Topic: %s", topic);
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {

    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    public String getId() {
        return "user-event-listener";
    }
}