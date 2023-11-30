package com.liaverg;

import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DbUtils {
    private static DataSource dataSource;
    private static final ThreadLocal<Connection> connection = ThreadLocal.withInitial(() -> null);
    private static final ThreadLocal<Boolean> isTransactionSuccessful = ThreadLocal.withInitial(() -> true);

    @FunctionalInterface
    public interface ConnectionConsumer {
        void accept(Connection connection) throws SQLException;
    }

    @FunctionalInterface
    public interface ConnectionFunction<T> {
        T apply(Connection connection) throws SQLException;
    }

    public static void initializeDatabase(DataSourceProvider dataSourceProvider) {
        dataSource = dataSourceProvider.createHikariProxyDataSource();
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
            throw new RuntimeException("Error during database connection", e);
        }
    }

    public static void executeStatementsInTransaction(ConnectionConsumer consumer) {
        boolean isOuterTransaction = false;

        try {
            if (!isTransactionActive()) {
                isOuterTransaction = true;
                startTransaction();
            }
            try {
                consumer.accept(connection.get());
            } catch (SQLException ex) {
                isTransactionSuccessful.set(false);
                throw new RuntimeException("Error during statement execution", ex);
            } finally {
                if (isOuterTransaction) {
                    completeOuterTransaction();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error during database connection", e);
        }
    }

    public static <T> T executeStatementsInTransactionWithResult(ConnectionFunction<T> consumer) {
        T result = null;
        boolean isOuterTransaction = false;
        try {
            if (!isTransactionActive()) {
                isOuterTransaction = true;
                startTransaction();
            }
            try {
                result = consumer.apply(connection.get());
            } catch (SQLException ex) {
                isTransactionSuccessful.set(false);
                throw new RuntimeException("Error during statement execution", ex);
            } finally {
                if (isOuterTransaction) {
                    completeOuterTransaction();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error during database connection", e);
        }
        return result;
    }

    private static boolean isTransactionActive() throws SQLException {
        return connection.get() != null;
    }

    private static void startTransaction() throws SQLException {
        isTransactionSuccessful.set(true);
        connection.set(dataSource.getConnection());
        connection.get().setAutoCommit(false);
    }

    private static void completeOuterTransaction() throws SQLException {
        try {
            if (isTransactionSuccessful.get()) {
                connection.get().commit();
            } else {
                connection.get().rollback();
            }
        } finally {
            connection.get().setAutoCommit(true);
            connection.get().close();
            connection.remove();
        }
    }
}
