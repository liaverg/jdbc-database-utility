package com.liaverg;

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

    public DataSource createDataSource(){
        PGSimpleDataSource pg_dataSource = new PGSimpleDataSource();
        pg_dataSource.setURL(url);
        pg_dataSource.setUser(username);
        pg_dataSource.setPassword(password);
        return pg_dataSource;
    }
}
