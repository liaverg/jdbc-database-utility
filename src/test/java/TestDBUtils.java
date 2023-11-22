import com.liaverg.DataSourceProvider;
import com.liaverg.DbUtils;
import com.liaverg.DbUtils.ConnectionConsumer;
import com.liaverg.DbUtils.ConnectionFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class TestDBUtils {

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    private static final String insertSQL = "INSERT INTO users_directory.users (username, email) VALUES (?, ?)";
    private static final String updateSQL = "UPDATE users_directory.users SET email = ? WHERE username = ?";
    private static final String selectSQL = "SELECT username, email FROM users_directory.users";

    private static final ConnectionConsumer successfulInsert = conn -> {
        try (PreparedStatement insertStatement = conn.prepareStatement(insertSQL)) {
            insertStatement.setString(1, "john_doe");
            insertStatement.setString(2, "john.doe@example.com");
            insertStatement.executeUpdate();
            insertStatement.setString(1, "jane_doe");
            insertStatement.setString(2, "jane.doe@example.com");
            insertStatement.executeUpdate();
        }
    };

    private static final ConnectionConsumer failedInsert = conn -> {
        try (PreparedStatement insertStatement = conn.prepareStatement(insertSQL)) {
            insertStatement.setString(1, "john_doe");
            insertStatement.setString(2, "john.doe@example.com");
            insertStatement.executeUpdate();
            throw new SQLException("Simulated exception during statement execution");
        }
    };

    private static final ConnectionFunction successfulUpdate = conn -> {
        try (PreparedStatement updateStatement = conn.prepareStatement(updateSQL)) {
            updateStatement.setString(1, "john.doe@gmail.com");
            updateStatement.setString(2, "john_doe");
            int updateStatementCount = updateStatement.executeUpdate();
            updateStatement.setString(1, "jane.doe@gmail.com");
            updateStatement.setString(2, "jane_doe");
            updateStatementCount += updateStatement.executeUpdate();
            return updateStatementCount;
        }
    };

    private static final ConnectionFunction failedUpdate = conn -> {
        try (PreparedStatement updateStatement = conn.prepareStatement(updateSQL)) {
            updateStatement.setString(1, "john.doe@gmail.com");
            updateStatement.setString(2, "john_doe");
            updateStatement.executeUpdate();
            throw new SQLException("Simulated exception during statement execution");
        }
    };

    private static final ConnectionConsumer successfulNestedInsert = conn -> {
        DbUtils.executeStatementsInTransaction(successfulInsert);
        try (PreparedStatement insertStatement = conn.prepareStatement(insertSQL)) {
            insertStatement.setString(1, "jake_doe");
            insertStatement.setString(2, "jake.doe@example.com");
            insertStatement.executeUpdate();
        }
    };

    private static final ConnectionConsumer failedInnerInsert = conn -> {
        DbUtils.executeStatementsInTransaction(failedInsert);
        try (PreparedStatement insertStatement = conn.prepareStatement(insertSQL)) {
            insertStatement.setString(1, "jake_doe");
            insertStatement.setString(2, "jake.doe@example.com");
            insertStatement.executeUpdate();
        }
    };

    private static final ConnectionConsumer failedOuterInsert = conn -> {
        DbUtils.executeStatementsInTransaction(successfulInsert);
        try (PreparedStatement insertStatement = conn.prepareStatement(insertSQL)) {
            insertStatement.setString(1, "jake_doe");
            insertStatement.setString(2, "jake.doe@example.com");
            insertStatement.executeUpdate();
            throw new SQLException("Simulated exception during statement execution");
        }
    };

    private static final ConnectionFunction successfulNestedUpdate = conn -> {
        int updateStatementCount = (int) DbUtils.executeStatementsInTransactionWithResult(successfulUpdate);
        try (PreparedStatement updateStatement = conn.prepareStatement(updateSQL)) {
            updateStatement.setString(1, "jane.doe@outlook.com");
            updateStatement.setString(2, "jane_doe");
            updateStatementCount += updateStatement.executeUpdate();
            return updateStatementCount;
        }
    };

    private static final ConnectionFunction failedInnerUpdate = conn -> {
        int updateStatementCount =  (int) DbUtils.executeStatementsInTransactionWithResult(failedUpdate);
        try (PreparedStatement updateStatement = conn.prepareStatement(updateSQL)) {
            updateStatement.setString(1, "jane.doe@outlook.com");
            updateStatement.setString(2, "jane_doe");
            updateStatementCount += updateStatement.executeUpdate();
            return updateStatementCount;
        }
    };

    private static final ConnectionFunction failedOuterUpdate = conn -> {
        DbUtils.executeStatementsInTransactionWithResult(successfulUpdate);
        try (PreparedStatement updateStatement = conn.prepareStatement(updateSQL)) {
            updateStatement.setString(1, "jane.doe@outlook.com");
            updateStatement.setString(2, "jane_doe");
            updateStatement.executeUpdate();
            throw new SQLException("Simulated exception during statement execution");
        }
    };


    private void assertUserRow(ResultSet resultSet, String expectedUsername, String expectedEmail) throws SQLException {
        assertTrue(resultSet.next(), "Expected another row in the ResultSet");
        assertEquals(expectedUsername, resultSet.getString("username"));
        assertEquals(expectedEmail, resultSet.getString("email"));
    }

    private boolean isConnectionOpen() throws SQLException {
        int connectionCount = 0;
        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement pgActiveStatement = conn.prepareStatement("SELECT * FROM pg_stat_activity WHERE state = 'active'")) {
                try (ResultSet resultSet = pgActiveStatement.executeQuery()) {
                    while (resultSet.next()) {
                        connectionCount++;
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return connectionCount != 1;
    }

    @BeforeAll
    static void setUp() {
        DataSourceProvider dataSourceProvider = new DataSourceProvider(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword()
        );
        DbUtils.setDataSource(dataSourceProvider.createDataSource());
        DbUtils.initializeSchema();
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            String truncateSQL = "TRUNCATE TABLE users_directory.users";
            try (PreparedStatement truncateStatement = conn.prepareStatement(truncateSQL)) {
                truncateStatement.executeUpdate();
            }
        }
    }

    @Test
    void should_insert_when_autocommit_on() throws Exception {
        assertDoesNotThrow(() -> DbUtils.executeStatements(successfulInsert));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@example.com");
                    assertUserRow(resultSet, "jane_doe", "jane.doe@example.com");
                }
            }
        }

        assertFalse(isConnectionOpen());
    }

    @Test
    void should_insert_data_only_from_successful_operations_when_autocommit_on() throws Exception {
        assertThrows(RuntimeException.class, () -> DbUtils.executeStatements(failedInsert));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@example.com");
                    assertFalse(resultSet.next());
                }
            }
        }

        assertFalse(isConnectionOpen());
    }

    @Test
    void should_insert_when_in_transaction() throws Exception {
        assertDoesNotThrow(() -> DbUtils.executeStatementsInTransaction(successfulInsert));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@example.com");
                    assertUserRow(resultSet, "jane_doe", "jane.doe@example.com");
                }
            }
        }

        assertFalse(isConnectionOpen());
    }

    @Test
    void should_fail_to_insert_when_in_transaction() throws Exception {
        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransaction(failedInsert));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertFalse(resultSet.next());
                }
            }
        }

        assertFalse(isConnectionOpen());
    }

    @Test
    void should_return_update_count_when_in_transaction() throws Exception {
        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            successfulInsert.accept(conn);
        }

        Object updatedRowsCount = DbUtils.executeStatementsInTransactionWithResult(successfulUpdate);
        assertEquals(2, updatedRowsCount);

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@gmail.com");
                    assertUserRow(resultSet, "jane_doe", "jane.doe@gmail.com");
                }
            }
        }

        assertFalse(isConnectionOpen());
    }

    @Test
    void should_fail_to_update_when_in_transaction() throws Exception {
        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            successfulInsert.accept(conn);
        }

        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransactionWithResult(failedUpdate));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@example.com");
                    assertUserRow(resultSet, "jane_doe", "jane.doe@example.com");
                }
            }
        }

        assertFalse(isConnectionOpen());
    }

    @Test
    void should_insert_when_in_nested_transaction() throws Exception {
        assertDoesNotThrow(() -> DbUtils.executeStatementsInTransaction(successfulNestedInsert));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@example.com");
                    assertUserRow(resultSet, "jane_doe", "jane.doe@example.com");
                    assertUserRow(resultSet, "jake_doe", "jake.doe@example.com");
                }
            }
        }

        assertFalse(isConnectionOpen());
    }

    @Test
    void should_fail_to_insert_when_inner_insert_fails_in_nested_transaction() throws Exception {
        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransaction(failedInnerInsert));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertFalse(resultSet.next());
                }
            }
        }

        assertFalse(isConnectionOpen());
    }

    @Test
    void should_fail_to_insert_when_outer_insert_fails_in_nested_transaction() throws Exception {
        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransaction(failedOuterInsert));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertFalse(resultSet.next());
                }
            }
        }

        assertFalse(isConnectionOpen());
    }

    @Test
    void should_return_update_count_when_in_nested_transaction() throws Exception {
        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            successfulInsert.accept(conn);
        }
        Object updatedRowsCount = DbUtils.executeStatementsInTransactionWithResult(successfulNestedUpdate);
        assertEquals(3, updatedRowsCount);

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@gmail.com");
                    assertUserRow(resultSet, "jane_doe", "jane.doe@outlook.com");
                }
            }
        }

        assertFalse(isConnectionOpen());
    }

    @Test
    void should_fail_to_update_when_inner_update_fails_in_nested_transaction() throws Exception {
        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            successfulInsert.accept(conn);
        }

        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransactionWithResult(failedInnerUpdate));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@example.com");
                    assertUserRow(resultSet, "jane_doe", "jane.doe@example.com");
                }
            }
        }

        assertFalse(isConnectionOpen());
    }

    @Test
    void should_fail_to_update_when_outer_update_fails_in_nested_transaction() throws Exception {
        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            successfulInsert.accept(conn);
        }

        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransactionWithResult(failedOuterUpdate));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@example.com");
                    assertUserRow(resultSet, "jane_doe", "jane.doe@example.com");
                }
            }
        }

        assertFalse(isConnectionOpen());
    }
}