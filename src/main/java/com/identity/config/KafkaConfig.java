package com.identity.config;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaConfig {
    
    private static Properties producerProps;

    static {
        initializeConfig();
    }

    private static void initializeConfig() {
        producerProps = new Properties();

        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EnvConfig.BOOTSTRAP_SERVERS_CONFIG);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, EnvConfig.CLIENT_ID_CONFIG);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                         "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                         "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, EnvConfig.ACKS_CONFIG);
        producerProps.put(ProducerConfig.RETRIES_CONFIG, EnvConfig.RETRIES_CONFIG);
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, EnvConfig.COMPRESSION_TYPE_CONFIG);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, EnvConfig.ENABLE_IDEMPOTENCE_CONFIG);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, EnvConfig.BATCH_SIZE_CONFIG);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, EnvConfig.LINGER_MS_CONFIG);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, EnvConfig.BUFFER_MEMORY_CONFIG);

        if (!"PLAINTEXT".equals(EnvConfig.SECURITY_PROTOCOL)) {
            producerProps.put("security.protocol", EnvConfig.SECURITY_PROTOCOL);
            producerProps.put("sasl.mechanism", EnvConfig.SASL_MECHANISM);
            producerProps.put("sasl.jaas.config", buildJaasConfig());
        }
    }

    private static String buildJaasConfig() {
        return String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            EnvConfig.SASL_USERNAME,
            EnvConfig.SASL_PASSWORD
        );
    }
    
    public static Properties getProducerProperties() {
        return producerProps;
    }
}
