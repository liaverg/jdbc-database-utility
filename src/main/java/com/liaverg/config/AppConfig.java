package com.liaverg.config;

import com.liaverg.utilities.DbUtils;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import java.sql.Connection;
import java.sql.SQLException;

public class AppConfig {
    private DataSourceProvider dataSourceProvider;
    private PropertiesReader propertiesReader;
    private static final int LEAK_DETECTION_THRESHOLD = 3000;

    public AppConfig() {
        this.propertiesReader = new PropertiesReader();
        this.dataSourceProvider = new DataSourceProvider(propertiesReader.getJdbcUrl(),
                propertiesReader.getUser(),
                propertiesReader.getPassword(),
                propertiesReader.getLeakDetectionThreshold());
        new DbUtils(dataSourceProvider.getHikariProxyDataSource());
        initializeSchema();
    }

    public AppConfig(String url, String username, String password) {
        this.dataSourceProvider = new DataSourceProvider(
                url, username, password, LEAK_DETECTION_THRESHOLD);
        new DbUtils(dataSourceProvider.getHikariProxyDataSource());
        initializeSchema();
    }

    public HikariDataSource getHikariDataSource(){
        return dataSourceProvider.getHikariDataSource();
    }

    private void initializeSchema() {
        try (Connection connection = dataSourceProvider.getHikariDataSource().getConnection()) {
            ScriptUtils.executeSqlScript(connection, new FileSystemResource("src/main/resources/schema.sql"));
        } catch (SQLException e) {
            throw new RuntimeException("error during database connection", e);
        }
    }

}
