package com.liaverg;

import com.liaverg.DbUtils.ConnectionConsumer;
import com.liaverg.DbUtils.ConnectionFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

public class Main {
    private static void insertUser(Connection conn, String username, String email) throws SQLException {
        String insertSQL = "INSERT INTO users_directory.users (username, email) VALUES (?, ?)";
        try (PreparedStatement insertStatement = conn.prepareStatement(insertSQL)) {
            insertStatement.setString(1, username);
            insertStatement.setString(2, email);
            insertStatement.executeUpdate();
        }
    }
    private static final ConnectionConsumer insertUsers = conn -> {
        insertUser(conn, "john_doe", "john.doe@example.com");
        insertUser(conn, "jane_doe", "jane.doe@example.com");
    };

    private static final ConnectionConsumer insertUsersWithNestedTransaction = conn -> {
        DbUtils.executeStatementsInTransaction(connection -> {
            insertUser(connection, "john_doe", "john.doe@example.com");
            insertUser(connection, "jane_doe", "jane.doe@example.com");
        });
        insertUser(conn, "jake_doe", "jake.doe@example.com");
        //throw new SQLException("Simulated exception during statement execution");
    };

    private static int updateUser(Connection conn, String username, String email) throws SQLException {
        String updateSQL = "UPDATE users_directory.users SET email = ? WHERE username = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateSQL)) {
            updateStatement.setString(1, email);
            updateStatement.setString(2, username);
            return updateStatement.executeUpdate();
        }
    }

    private static final ConnectionFunction updateUserEmails = conn -> {
        int updateCount = 0;
        updateCount += updateUser(conn, "john.doe@gmail.com","john_doe");
        updateCount += updateUser(conn, "jane.doe@gmail.com","jane_doe");
        return updateCount;
    };

    private static final ConnectionFunction selectUsers = conn -> {
        String selectSQL = "SELECT username, email FROM users_directory.users";
        try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
            try (ResultSet resultSet = selectStatement.executeQuery()) {
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
        DbUtils.initializeDatabase(new DataSourceProvider());

        DbUtils.executeStatementsInTransaction(insertUsersWithNestedTransaction);
//        DbUtils.executeStatements(insertUsers);
//        DbUtils.executeStatementsInTransaction(insertUsers);
//        Object updatedRowsCount = DbUtils.executeStatementsInTransactionWithResult(updateUserEmails);
//        System.out.println("Number of Statements Updated: " + updatedRowsCount);
        Object usersSet = DbUtils.executeStatementsInTransactionWithResult(selectUsers);
        for (String [] userInfo: (HashSet<String[]>) usersSet){
            System.out.println("Username: " + userInfo[0] + "\tEmail: " + userInfo[1]);
        }
    }
}