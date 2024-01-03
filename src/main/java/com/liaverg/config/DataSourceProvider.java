package com.liaverg.config;

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
    private final HikariDataSource hikariDataSource;
    private  final DataSource hikariProxyDataSource;

    public DataSourceProvider(String url, String username, String password,
                              int leakDetectionThreshold) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.leakDetectionThreshold = leakDetectionThreshold;
        hikariDataSource = createHikariDataSource();
        hikariProxyDataSource = createHikariProxyDataSource();
    }

    private HikariDataSource createHikariDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setLeakDetectionThreshold(leakDetectionThreshold);
        return new HikariDataSource(config);
    }

    private DataSource createHikariProxyDataSource() {
        return ProxyDataSourceBuilder
                .create(hikariDataSource)
                .name(url)
                .logQueryBySlf4j(SLF4JLogLevel.INFO)
                .multiline()
                .countQuery()
                .traceMethods()
                .build();
    }

    public HikariDataSource getHikariDataSource() {
        return hikariDataSource;
    }

    public DataSource getHikariProxyDataSource() {
        return hikariProxyDataSource;
    }
}
