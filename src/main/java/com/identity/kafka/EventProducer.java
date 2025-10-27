package com.identity.kafka;

import com.identity.config.EnvConfig;
import com.identity.config.KafkaConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jboss.logging.Logger;

public class EventProducer {
    
    private static final Logger logger = Logger.getLogger(EventProducer.class);
    private static volatile Producer<String, String> producer;
    private static final Object lock = new Object();
    
    public EventProducer() {
        if (producer == null) {
            synchronized (lock) {
                if (producer == null) {
                    logger.info("Creating singleton Kafka Producer");
                    producer = new KafkaProducer<>(KafkaConfig.getProducerProperties());
                    logger.info("Kafka Producer created successfully");
                }
            }
        }
    }
    
    public void sendUserEvent(String key, String value) {
        sendMessage(EnvConfig.USER_EVENTS_TOPIC, key, value);
    }
    
    public void sendAdminEvent(String key, String value) {
        sendMessage(EnvConfig.ADMIN_EVENTS_TOPIC, key, value);
    }
    
    private void sendMessage(String topic, String key, String value) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.errorf(exception, "Failed to send to Kafka: topic=%s, key=%s", topic, key);
                } else {
                    logger.infof("Message sent: topic=%s, partition=%d, offset=%d, key=%s", 
                                metadata.topic(), 
                                metadata.partition(), 
                                metadata.offset(),
                                key);
                }
            });
        } catch (Exception e) {
            logger.errorf(e, "Error sending message to Kafka: topic=%s", topic);
        }
    }
    
    public void close() {
        logger.debug("EventProducer.close() called - keeping singleton producer alive");
    }
    
    public static void shutdown() {
        if (producer != null) {
            synchronized (lock) {
                if (producer != null) {
                    logger.info("Shutting down Kafka Producer");
                    producer.close();
                    producer = null;
                }
            }
        }
    }
}