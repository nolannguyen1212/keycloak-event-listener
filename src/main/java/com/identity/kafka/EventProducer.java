package com.identity.kafka;

import com.identity.kafka.config.EnvConfig;
import com.identity.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jboss.logging.Logger;

public class EventProducer {

    private static final Logger log = Logger.getLogger(EventProducer.class);
    private static volatile Producer<String, String> producer;

    public EventProducer() {
        if (producer == null) {
            synchronized (EventProducer.class) {
                if (producer == null) {
                    producer = new KafkaProducer<>(KafkaConfig.get());
                    log.info("Kafka producer initialized");
                }
            }
        }
    }

    public void sendUser(String key, String value) {
        send(EnvConfig.USER_TOPIC, key, value);
    }

    public void sendAdmin(String key, String value) {
        send(EnvConfig.ADMIN_TOPIC, key, value);
    }

    private void send(String topic, String key, String value) {
        try {
            producer.send(new ProducerRecord<>(topic, key, value), (m, e) -> {
                if (e != null)
                    log.errorf(e, "Send failed: %s/%s", topic, key);
            });
        } catch (Exception e) {
            log.errorf(e, "Error sending: %s", topic);
        }
    }

    public void close() {
    }

    public static void shutdown() {
        if (producer != null) {
            synchronized (EventProducer.class) {
                if (producer != null) {
                    log.info("Shutting down producer");
                    producer.close();
                    producer = null;
                }
            }
        }
    }
}