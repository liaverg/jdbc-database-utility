package com.liaverg;
import java.io.IOException;
import java.util.Properties;
import java.net.URL;

public class PropertiesReader {
    private final Properties properties;

    public PropertiesReader() {
        this.properties = new Properties();
        readProperties();
    }

    private void readProperties(){
        URL url = ClassLoader.getSystemResource("datasource.properties");
        try {
            properties.load(url.openStream());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public String getJdbcUrl() {
        return properties.getProperty("jdbcUrl");
    }

    public String getUser() {
        return properties.getProperty("user");
    }

    public String getPassword() {
        return properties.getProperty("password");
    }

    public int getLeakDetectionThreshold() {
        return Integer.parseInt(properties.getProperty("leakDetectionThreshold"));
    }
}