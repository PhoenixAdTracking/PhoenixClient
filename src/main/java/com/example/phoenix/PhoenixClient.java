package com.example.phoenix;

import com.example.phoenix.authorization.KeycloakServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(exclude = LiquibaseAutoConfiguration.class)
@EnableConfigurationProperties(KeycloakServerProperties.class)
public class PhoenixClient {

	private static final Logger LOG = LoggerFactory.getLogger(PhoenixClient.class);

	public static void main(String[] args) {
		SpringApplication.run(PhoenixClient.class, args);
	}

	@Bean
	ApplicationListener<ApplicationReadyEvent> onApplicationReadyEventListener(
			ServerProperties serverProperties, KeycloakServerProperties keycloakServerProperties) {
		return (evt) -> {
			Integer port = serverProperties.getPort();
			String keycloakContextPath = keycloakServerProperties.getContextPath();
			LOG.info("Embedded Keycloak started: http://localhost:{}{} to use keycloak",
					port, keycloakContextPath);
		};
	}
}
