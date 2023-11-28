package com.liaverg;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DataSourceProvider {
    private final String url;
    private final String username;
    private final String password;

    public DataSourceProvider(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public DataSourceProvider() {
        url = null;
        username = null;
        password = null;
    }

    public boolean isDataSourceProviderInitialized(){
        return !(url == null && username == null && password == null);
    }

    public HikariDataSource createHikariDataSourceFromProperties(){
        HikariConfig config = new HikariConfig("/datasource.properties");
        return new HikariDataSource(config);
    }

    public HikariDataSource createHikariDataSource(){
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setLeakDetectionThreshold(3000);
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }
}
