package com.liaverg;

import org.postgresql.ds.PGSimpleDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Main {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/mydb";
    private static final String USER = "postgres";
    private static final String PASSWORD = "root";

    public static void main(String[] args) {
        PGSimpleDataSource pg_dataSource = new PGSimpleDataSource();
        pg_dataSource.setURL(DB_URL);
        pg_dataSource.setUser(USER);
        pg_dataSource.setPassword(PASSWORD);
        DbUtils dbUtils = new DbUtils(pg_dataSource);

        DbUtils.ConnectionConsumer insertData = conn -> {
            String insertSQL = "INSERT INTO users_directory.users (username, email) VALUES (?, ?)";
            try (PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {
                insertStmt.setString(1, "john_doe");
                insertStmt.setString(2, "john.doe@example.com");
                insertStmt.executeUpdate();
                insertStmt.setString(1, "jane_doe");
                insertStmt.setString(2, "jane.doe@example.com");
                insertStmt.executeUpdate();
            }
        };
    }
}