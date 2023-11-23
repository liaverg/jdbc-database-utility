package com.liaverg;

import com.liaverg.DbUtils.ConnectionConsumer;
import com.liaverg.DbUtils.ConnectionFunction;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

public class Main {
    private static final ConnectionConsumer insertData = conn -> {
        String insertSQL = "INSERT INTO users_directory.users (username, email) VALUES (?, ?)";
        try (PreparedStatement insertStatement = conn.prepareStatement(insertSQL)) {
            insertStatement.setString(1, "john_doe");
            insertStatement.setString(2, "john.doe@example.com");
            insertStatement.executeUpdate();
            insertStatement.setString(1, "jane_doe");
            insertStatement.setString(2, "jane.doe@example.com");
            insertStatement.executeUpdate();
        }
    };

    private static final ConnectionConsumer insertDataWithNestedTransaction = conn -> {
        DbUtils.executeStatementsInTransaction(insertData);
        String insertSQL = "INSERT INTO users_directory.users (username, email) VALUES (?, ?)";
        try (PreparedStatement insertStatement = conn.prepareStatement(insertSQL)) {
            insertStatement.setString(1, "jake_doe");
            insertStatement.setString(2, "jake.doe@example.com");
            insertStatement.executeUpdate();
            //throw new SQLException("Simulated exception during statement execution");
        }
    };

    private static final ConnectionFunction updateData = conn -> {
        String updateSQL = "UPDATE users_directory.users SET email = ? WHERE username = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateSQL)) {
            updateStatement.setString(1, "john.doe@gmail.com");
            updateStatement.setString(2, "john_doe");
            int updateCount = updateStatement.executeUpdate();
            updateStatement.setString(1, "jane.doe@gmail.com");
            updateStatement.setString(2, "jane_doe");
            updateCount += updateStatement.executeUpdate();
            return updateCount;
        }
    };

    private static final ConnectionFunction selectData = conn -> {
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

        DbUtils.executeStatementsInTransaction(insertDataWithNestedTransaction);
//        DbUtils.executeStatements(insertData);
//        DbUtils.executeStatementsInTransaction(insertData);
//        Object updatedRowsCount = DbUtils.executeStatementsInTransactionWithResult(updateData);
//        System.out.println("Number of Statements Updated: " + updatedRowsCount);
        Object usersSet = DbUtils.executeStatementsInTransactionWithResult(selectData);
        for (String [] userInfo: (HashSet<String[]>) usersSet){
            System.out.println("Username: " + userInfo[0] + "\tEmail: " + userInfo[1]);
        }
    }
}