package com.liaverg.config;

import com.liaverg.utilities.DbUtils;

public class AppConfig {
    private DataSourceProvider dataSourceProvider;
    private PropertiesReader propertiesReader;

    public AppConfig() {
        this.propertiesReader = new PropertiesReader();
        this.dataSourceProvider = new DataSourceProvider(
                propertiesReader.getJdbcUrl(),
                propertiesReader.getUser(),
                propertiesReader.getPassword(),
                propertiesReader.getLeakDetectionThreshold());
        new DbUtils(dataSourceProvider.getHikariProxyDataSource());
    }
}
