package com.liaverg;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;

public class DbUtils {
    private static HikariDataSource dataSource;
    private static final ThreadLocal<Connection> connection = ThreadLocal.withInitial(() -> null);
    private static final ThreadLocal<Boolean> isTransactionSuccessful = ThreadLocal.withInitial(() -> true);
    public static final Logger log = LoggerFactory.getLogger(DbUtils.class);

    @FunctionalInterface
    public interface ConnectionConsumer {
        void accept(Connection connection) throws SQLException;
    }

    @FunctionalInterface
    public interface ConnectionFunction<T> {
        T apply(Connection connection) throws SQLException;
    }

    public static void initializeDatabase(DataSourceProvider dataSourceProvider) {
        log.info("Initializing database {}", LocalDateTime.now());
        if (dataSourceProvider.isDataSourceProviderInitialized()) {
            log.info("Initialize from fields of datasource");
            dataSource = dataSourceProvider.createHikariDataSource();
        } else {
            log.info("Initialize from properties");
            dataSource = dataSourceProvider.createHikariDataSourceFromProperties();
        }
        initializeSchema();
    }

    private static void initializeSchema() {
        try (Connection connection = dataSource.getConnection()) {
            ScriptUtils.executeSqlScript(connection, new FileSystemResource("src/main/resources/schema.sql"));
        } catch (SQLException e) {
            log.info("Exception thrown during database connection");
            throw new RuntimeException("error during database connection", e);
        }
    }

    public static void executeStatements(ConnectionConsumer consumer) {
        log.info("Connecting to the database");
        try (Connection connection = dataSource.getConnection()) {
            try {
                log.info("Executing statements");
                consumer.accept(connection);
            } catch (SQLException ex) {
                log.info("Exception thrown during execution");
                throw new RuntimeException("error during statement execution", ex);
            }
        } catch (SQLException e) {
            handleConnectionException(e);
        }
        log.info("Closed connection to the database");
    }

    public static void executeStatementsInTransaction(ConnectionConsumer consumer) {
        boolean isOuterTransaction = false;

        try {
            if (!isTransactionActive()) {
                isOuterTransaction = true;
                startTransaction();
            }

            try {
                log.info("Executing statements");
                consumer.accept(connection.get());
            } catch (SQLException ex) {
                handleTransactionException(ex);
            } finally {
                if (isOuterTransaction) {
                    completeOuterTransaction();
                }
            }
        } catch (SQLException e) {
            handleConnectionException(e);
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
                log.info("Executing statements");
                result = consumer.apply(connection.get());
            } catch (SQLException ex) {
                handleTransactionException(ex);
            } finally {
                if (isOuterTransaction) {
                    completeOuterTransaction();
                }
            }
        } catch (SQLException e) {
            handleConnectionException(e);
        }
        return result;
    }

    private static boolean isTransactionActive() throws SQLException {
        return connection.get() != null;
    }

    private static void startTransaction() throws SQLException {
        log.info("Connecting to the database");
        isTransactionSuccessful.set(true);
        connection.set(dataSource.getConnection());
        connection.get().setAutoCommit(false);
    }

    private static void completeOuterTransaction() throws SQLException {
        log.info("Finally block executed for outer transaction");
        try {
            if (isTransactionSuccessful.get()) {
                log.info("Committing statements");
                connection.get().commit();
            } else {
                log.info("Rolling back statements");
                connection.get().rollback();
            }
        } finally {
            releaseResources();
        }
    }

    private static void handleTransactionException(SQLException ex) {
        log.info("Exception thrown during execution");
        isTransactionSuccessful.set(false);
        throw new RuntimeException("Error during statement execution", ex);
    }

    private static void handleConnectionException(SQLException e) {
        log.info("Exception thrown during database connection");
        throw new RuntimeException("Error during database connection", e);
    }

    private static void releaseResources() throws SQLException {
        log.info("Releasing resources");
        if (connection.get() != null) {
            log.info("Closing connection to the database");
            connection.get().setAutoCommit(true);
            connection.get().close();
            connection.remove();
        }
        log.info("Resources released");
    }
}
