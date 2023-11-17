package com.liaverg;

import javax.sql.DataSource;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;

public class DbUtils {

    private static DataSource dataSource;

    public static void setDataSource(DataSource dataSource) {
        DbUtils.dataSource = dataSource;
    }

    @FunctionalInterface
    public interface ConnectionConsumer {
        void accept(Connection connection) throws SQLException;
    }

    @FunctionalInterface
    public interface ConnectionFunction<T> {
        T apply(Connection connection) throws SQLException;
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
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                consumer.accept(connection);
                connection.commit();
            } catch (SQLException ex) {
                try {
                    connection.rollback();
                } catch (SQLException e) {
                    throw new RuntimeException("error during rollback", e);
                }
                throw new RuntimeException("error during statement execution", ex);
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new RuntimeException("error during database connection", e);
        }
    }

    public static <T> T executeStatementsInTransactionWithResult(ConnectionFunction<T> consumer) {
        T result = null;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                result = consumer.apply(connection);
                connection.commit();
            } catch (SQLException ex) {
                try {
                    connection.rollback();
                } catch (SQLException e) {
                    throw new RuntimeException("error during rollback", e);
                }
                throw new RuntimeException("error during statement execution", ex);
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new RuntimeException("error during database connection", e);
        }
        return result;
    }
}
