package com.identity.config;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class EnvConfig {
    
    public static final String BOOTSTRAP_SERVERS_CONFIG;
    public static final String CLIENT_ID_CONFIG;
    public static final String ACKS_CONFIG;
    public static final int RETRIES_CONFIG;
    public static final String COMPRESSION_TYPE_CONFIG;
    public static final boolean ENABLE_IDEMPOTENCE_CONFIG;
    public static final int BATCH_SIZE_CONFIG;
    public static final int LINGER_MS_CONFIG;
    public static final int BUFFER_MEMORY_CONFIG;
    
    public static final String USER_EVENTS_TOPIC;
    public static final String ADMIN_EVENTS_TOPIC;
    
    public static final String SECURITY_PROTOCOL;
    public static final String SASL_MECHANISM;
    public static final String SASL_USERNAME;
    public static final String SASL_PASSWORD;
    
    public static final List<String> ALLOWED_REALMS;
    
    static {
        BOOTSTRAP_SERVERS_CONFIG = getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        CLIENT_ID_CONFIG = getEnv("KAFKA_CLIENT_ID", "keycloak");
        ACKS_CONFIG = getEnv("KAFKA_ACKS", "all");
        RETRIES_CONFIG = getEnvInt("KAFKA_RETRIES", 3);
        COMPRESSION_TYPE_CONFIG = getEnv("KAFKA_COMPRESSION", "gzip");
        ENABLE_IDEMPOTENCE_CONFIG = getEnvBoolean("KAFKA_ENABLE_IDEMPOTENCE", true);
        BATCH_SIZE_CONFIG = getEnvInt("KAFKA_BATCH_SIZE", 16384);
        LINGER_MS_CONFIG = getEnvInt("KAFKA_LINGER_MS", 10);
        BUFFER_MEMORY_CONFIG = getEnvInt("KAFKA_BUFFER_MEMORY", 33554432);
        
        USER_EVENTS_TOPIC = getEnv("KAFKA_USER_EVENTS_TOPIC", "keycloak-user-events");
        ADMIN_EVENTS_TOPIC = getEnv("KAFKA_ADMIN_EVENTS_TOPIC", "keycloak-admin-events");
        
        SECURITY_PROTOCOL = getEnv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
        SASL_MECHANISM = getEnv("KAFKA_SASL_MECHANISM", "PLAIN");
        SASL_USERNAME = getEnv("KAFKA_SASL_USERNAME", "");
        SASL_PASSWORD = getEnv("KAFKA_SASL_PASSWORD", "");

        ALLOWED_REALMS = getEnvList("KAFKA_ALLOWED_REALMS", ",");
    }
    
    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }
    
    private static int getEnvInt(String key, int defaultValue) {
        String value = System.getenv(key);
        try {
            return value != null ? Integer.parseInt(value) : defaultValue;
        } catch (NumberFormatException e) {
            System.err.println("Invalid integer value for " + key + ": " + value);
            return defaultValue;
        }
    }
    
    private static boolean getEnvBoolean(String key, boolean defaultValue) {
        String value = System.getenv(key);
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }
    
    private static List<String> getEnvList(String key, String delimiter) {
        String value = getEnv(key, "");
        if (value.isEmpty()) {
            return List.of();
        }
        return Arrays.stream(value.split(delimiter))
                     .map(String::trim)
                     .filter(s -> !s.isEmpty())
                     .collect(Collectors.toList());
    }
    
    public static boolean isRealmAllowed(String realm) {
        return ALLOWED_REALMS.isEmpty() || 
               ALLOWED_REALMS.contains("*") || 
               ALLOWED_REALMS.contains(realm);
    }
}