package com.liaverg;

import com.liaverg.DbUtils.ConnectionConsumer;
import com.liaverg.DbUtils.ConnectionFunction;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;

public class Main {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/mydb";
    private static final String USER = "postgres";
    private static final String PASSWORD = "root";

    private static final ConnectionConsumer insertData = conn -> {
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
    private static final ConnectionFunction selectData = conn -> {
        String selectSQL = "SELECT username, email FROM users_directory.users";
        try (PreparedStatement selectStmt = conn.prepareStatement(selectSQL)) {
            try (ResultSet resultSet = selectStmt.executeQuery()) {
                HashSet<String []> usersSet= new HashSet<>();
                while (resultSet.next()) {
                    String username = resultSet.getString("username");
                    String email = resultSet.getString("email");
                    usersSet.add(new String[]{username, email});
                }
                return usersSet;
            }
        }
    };

    public static void main(String[] args) {
        DataSourceProvider dataSourceProvider = new DataSourceProvider(DB_URL, USER, PASSWORD);
        DbUtils.setDataSource(dataSourceProvider.createDataSource());
        DbUtils.initializeSchema();

        DbUtils.executeStatements(insertData);
        DbUtils.executeStatementsInTransaction(insertData);
        HashSet<String []> usersSet = (HashSet<String[]>) DbUtils.executeStatementsInTransactionWithResult(selectData);
        for (String [] userInfo: usersSet){
            System.out.println("Username: " + userInfo[0] + "\tEmail: " + userInfo[1]);
        }
    }
}