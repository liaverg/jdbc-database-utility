package com.liaverg;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

import javax.sql.DataSource;

public class DataSourceProvider {
    private final String url;
    private final String username;
    private final String password;
    private final int leakDetectionThreshold;

    public DataSourceProvider(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.leakDetectionThreshold = 3000;
    }

    public DataSourceProvider() {
        PropertiesReader propertiesReader = new PropertiesReader();
        this.url = propertiesReader.getJdbcUrl();
        this.username = propertiesReader.getUser();
        this.password = propertiesReader.getPassword();
        this.leakDetectionThreshold = propertiesReader.getLeakDetectionThreshold();
    }

    public HikariDataSource createHikariDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setLeakDetectionThreshold(leakDetectionThreshold);
        return new HikariDataSource(config);
    }

    public DataSource createHikariProxyDataSource() {
        HikariDataSource dataSource = createHikariDataSource();
        return ProxyDataSourceBuilder
                .create(dataSource)
                .name(url)
                .logQueryBySlf4j(SLF4JLogLevel.INFO)
                .multiline()
                .countQuery()
                .traceMethods()
                .build();
    }


}
