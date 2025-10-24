package org.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;

import java.util.Properties;

public class KafkaManager {

    private static final Logger log = Logger.getLogger(KafkaManager.class);
    private static KafkaProducer<String, String> instance;

    public static synchronized KafkaProducer<String, String> getProducer() {
        if (instance == null) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS"));
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "keycloak");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, env("KAFKA_ACKS"));
            props.put(ProducerConfig.RETRIES_CONFIG, envInt("KAFKA_RETRIES"));
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, env("KAFKA_COMPRESSION"));
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

            instance = new KafkaProducer<>(props);
            log.infof("Kafka initialized: %s", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        }
        return instance;
    }

    public static synchronized void close() {
        if (instance != null) {
            try {
                instance.close();
                instance = null;
            } catch (Exception e) {
                log.error("Kafka close error", e);
            }
        }
    }

    private static String env(String key) {
        return System.getenv(key);
    }

    private static int envInt(String key) {
        return Integer.parseInt(System.getenv(key));
    }
}