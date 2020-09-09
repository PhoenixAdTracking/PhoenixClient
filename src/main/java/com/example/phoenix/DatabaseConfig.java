package com.example.phoenix;

import com.example.phoenix.ingestion.PhoenixDataProcessor;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


import java.net.URISyntaxException;

@Configuration
public class DatabaseConfig {
    @Bean
    public BasicDataSource basicDataSource() {
        String dbUrl = System.getenv("JDBC_DATABASE_URL");
        String username = System.getenv("JDBC_DATABASE_USERNAME");
        String password = System.getenv("JDBC_DATABASE_PASSWORD");

        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        basicDataSource.setUrl(dbUrl);
        basicDataSource.setUsername(username);
        basicDataSource.setPassword(password);
        return basicDataSource;
    }

    @Bean(name = "PhoenixDB")
    public PhoenixDataProcessor dataSource() throws URISyntaxException {
        return new PhoenixDataProcessor(basicDataSource());
    }
}
