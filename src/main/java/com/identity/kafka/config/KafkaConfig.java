package com.identity.kafka.config;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaConfig {

    private static Properties props;

    static {
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EnvConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, EnvConfig.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, EnvConfig.ACKS);
        props.put(ProducerConfig.RETRIES_CONFIG, EnvConfig.RETRIES);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, EnvConfig.COMPRESSION);

        if (!"PLAINTEXT".equals(EnvConfig.SECURITY_PROTOCOL)) {
            props.put("security.protocol", EnvConfig.SECURITY_PROTOCOL);
            props.put("sasl.mechanism", EnvConfig.SASL_MECHANISM);
            props.put("sasl.jaas.config", String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    EnvConfig.SASL_USERNAME, EnvConfig.SASL_PASSWORD));
        }
    }

    public static Properties get() {
        return props;
    }
}