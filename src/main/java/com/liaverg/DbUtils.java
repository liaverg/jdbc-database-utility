package com.liaverg;

import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DbUtils {
    private static DataSource dataSource;
    private static final ThreadLocal<Integer> transactionDepth = ThreadLocal.withInitial(() -> -1);
    private static final ThreadLocal<Boolean> isTransactionSuccessful = new ThreadLocal<>();
    private static Connection connection = null;

    @FunctionalInterface
    public interface ConnectionConsumer {
        void accept(Connection connection) throws SQLException;
    }

    @FunctionalInterface
    public interface ConnectionFunction<T> {
        T apply(Connection connection) throws SQLException;
    }

    public static void initializeDatabase(DataSourceProvider dataSourceProvider){
        dataSource = dataSourceProvider.createDataSource();
        initializeSchema();
    }

    private static void initializeSchema() {
        try (Connection connection = dataSource.getConnection()) {
            ScriptUtils.executeSqlScript(connection, new FileSystemResource("src/main/resources/schema.sql"));
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
}
