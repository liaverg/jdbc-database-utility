package com.liaverg;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

public class PropertiesReader {

    private final Properties properties;

    public PropertiesReader() {
        this.properties = new Properties();
        readProperties();
    }

    private void readProperties(){
        java.net.URL url = ClassLoader.getSystemResource("database.properties");
        try {
            properties.load(url.openStream());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public String getPropertiesDbUrl() {
        return properties.getProperty("DB_URL");
    }

    public String getPropertiesUser() {
        return properties.getProperty("USER");
    }

    public String getPropertiesPassword() {
        return properties.getProperty("PASSWORD");
    }
}