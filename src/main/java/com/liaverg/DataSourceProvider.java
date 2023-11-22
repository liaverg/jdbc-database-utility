package com.liaverg;

import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

public class DataSourceProvider {
    private final String url;
    private final String username;
    private final String password;

    public DataSourceProvider() {
        PropertiesReader propertiesReader = new PropertiesReader();
        this.url = propertiesReader.getPropertiesDbUrl();
        this.username = propertiesReader.getPropertiesUser();
        this.password = propertiesReader.getPropertiesPassword();
    }

    public DataSourceProvider(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public DataSource createDataSource() {
        PGSimpleDataSource pg_dataSource = new PGSimpleDataSource();
        pg_dataSource.setURL(url);
        pg_dataSource.setUser(username);
        pg_dataSource.setPassword(password);
        return pg_dataSource;
    }
}
