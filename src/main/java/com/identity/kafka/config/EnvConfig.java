package com.identity.kafka.config;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class EnvConfig {

    public static final String BOOTSTRAP_SERVERS = getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    public static final String CLIENT_ID = getEnv("KAFKA_CLIENT_ID", "keycloak");
    public static final String ACKS = getEnv("KAFKA_ACKS", "all");
    public static final int RETRIES = getInt("KAFKA_RETRIES", 3);
    public static final String COMPRESSION = getEnv("KAFKA_COMPRESSION", "gzip");

    public static final String USER_TOPIC = getEnv("KAFKA_USER_TOPIC", "keycloak-user-events");
    public static final String ADMIN_TOPIC = getEnv("KAFKA_ADMIN_TOPIC", "keycloak-admin-events");

    public static final String SECURITY_PROTOCOL = getEnv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
    public static final String SASL_MECHANISM = getEnv("KAFKA_SASL_MECHANISM", "PLAIN");
    public static final String SASL_USERNAME = getEnv("KAFKA_SASL_USERNAME", "");
    public static final String SASL_PASSWORD = getEnv("KAFKA_SASL_PASSWORD", "");

    public static final List<String> ALLOWED_REALMS = getList("KAFKA_ALLOWED_REALMS");
    public static final String CONFIG_FILE = getEnv("KAFKA_CONFIG_FILE", "events-config.json");

    private static String getEnv(String key, String def) {
        String val = System.getenv(key);
        return val != null ? val : def;
    }

    private static int getInt(String key, int def) {
        try {
            String val = System.getenv(key);
            return val != null ? Integer.parseInt(val) : def;
        } catch (Exception e) {
            return def;
        }
    }

    private static List<String> getList(String key) {
        String val = System.getenv(key);
        if (val == null || val.isEmpty())
            return List.of();
        return Arrays.stream(val.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    public static boolean isRealmAllowed(String realm) {
        return ALLOWED_REALMS.isEmpty() || ALLOWED_REALMS.contains("*") || ALLOWED_REALMS.contains(realm);
    }
}
