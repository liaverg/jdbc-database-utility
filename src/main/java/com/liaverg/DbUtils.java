package com.liaverg;

import javax.sql.DataSource;
import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;

public class DbUtils {

    private static DataSource dataSource;

    @Inject
    public DbUtils(DataSource dataSource) {
        DbUtils.dataSource = dataSource;
    }

    @FunctionalInterface
    public interface ConnectionConsumer {
        void accept(Connection connection) throws SQLException;
    }

    public static void executeStatements(ConnectionConsumer consumer){
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
}
