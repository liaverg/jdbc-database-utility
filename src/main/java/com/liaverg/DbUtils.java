package com.liaverg;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;


public class DbUtils {
    private static DataSource dataSource;
    private static final ThreadLocal<Integer> transactionDepth = ThreadLocal.withInitial(() -> -1);
    private static final ThreadLocal<Boolean> isTransactionSuccessful = new ThreadLocal<>();
    private static Connection connection = null;
    private static final String SCHEMA = "schema.sql";

    @FunctionalInterface
    public interface ConnectionConsumer {
        void accept(Connection connection) throws SQLException;
    }

    @FunctionalInterface
    public interface ConnectionFunction<T> {
        T apply(Connection connection) throws SQLException;
    }

    public static void setDataSource(DataSource dataSource) {
        DbUtils.dataSource = dataSource;
    }

    public static DataSource getDataSource() {
        return dataSource;
    }

    public static void initializeSchema() {
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner scriptRunner = new ScriptRunner(connection);
            scriptRunner.setDelimiter(";");
            try (Reader scriptReader = Resources.getResourceAsReader(SCHEMA)) {
                scriptRunner.runScript(scriptReader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("error during database connection", e);
        }
    }

    public static void executeStatements(ConnectionConsumer consumer) {
        try (Connection connection = dataSource.getConnection()) {
            try {
                consumer.accept(connection);
            } catch (SQLException ex) {
                throw new RuntimeException("error during statement execution", ex);
            }
        } catch (SQLException e) {
            throw new RuntimeException("error during database connection", e);
        }
    }

    public static void executeStatementsInTransaction(ConnectionConsumer consumer) {
        try {
            transactionDepth.set(transactionDepth.get() + 1);
            if (transactionDepth.get() == 0) {
                isTransactionSuccessful.set(true);
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
            }
            try {
                consumer.accept(connection);
            } catch (SQLException ex) {
                isTransactionSuccessful.set(false);
                throw new RuntimeException("error during statement execution", ex);
            } finally {
                completeTransaction();
            }
        } catch (SQLException e) {
            throw new RuntimeException("error during database connection", e);
        }
    }

    public static <T> T executeStatementsInTransactionWithResult(ConnectionFunction<T> consumer) {
        T result;
        try {
            transactionDepth.set(transactionDepth.get() + 1);
            if (transactionDepth.get() == 0) {
                isTransactionSuccessful.set(true);
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
            }
            try {
                result = consumer.apply(connection);
            } catch (SQLException ex) {
                isTransactionSuccessful.set(false);
                throw new RuntimeException("error during statement execution", ex);
            } finally {
                completeTransaction();
            }
        } catch (SQLException e) {
            throw new RuntimeException("error during database connection", e);
        }
        return result;
    }

    private static void completeTransaction() throws SQLException {
        if (transactionDepth.get() == 0){
            if (isTransactionSuccessful.get())
                connection.commit();
            else{
                try {
                    connection.rollback();
                } catch (SQLException e) {
                    throw new RuntimeException("error during rollback", e);
                }
            }
            if(connection != null){
                connection.setAutoCommit(true);
                connection.close();
            }
        }
        transactionDepth.set(transactionDepth.get() - 1);
    }

    private static void printConnections(Connection connection){
        try (PreparedStatement pgActiveStatement = connection.prepareStatement("SELECT * FROM pg_stat_activity WHERE state = 'active'")) {
            try (ResultSet resultSet = pgActiveStatement.executeQuery()) {
                while (resultSet.next()) {
                    System.out.println(resultSet.getString("datname") + ' ' + resultSet.getString("pid"));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
