package com.liaverg;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;

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
        return new HikariDataSource(config);
    }

    public DataSource createDataSource() {
        PGSimpleDataSource pg_dataSource = new PGSimpleDataSource();
        pg_dataSource.setURL(url);
        pg_dataSource.setUser(username);
        pg_dataSource.setPassword(password);
        return pg_dataSource;
    }
}
