package com.example.phoenix.authorization;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "keycloak.server")
public class KeycloakServerProperties {
    String contextPath = "/auth";
    String realmImportFile = "baeldung-realm.json";
    AdminUser adminUser = new AdminUser();

    // getters and setters

    @Data
    public static class AdminUser {
        String username = "admin";
        String password = "admin";

        // getters and setters
    }
}