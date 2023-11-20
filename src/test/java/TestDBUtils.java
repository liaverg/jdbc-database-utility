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

    private static final ConnectionConsumer successfulInsert = conn -> {
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

    private static final ConnectionConsumer failedInsert = conn -> {
        String insertSQL = "INSERT INTO users_directory.users (username, email) VALUES (?, ?)";
        try (PreparedStatement insertStatement = conn.prepareStatement(insertSQL)) {
            insertStatement.setString(1, "john_doe");
            insertStatement.setString(2, "john.doe@example.com");
            insertStatement.executeUpdate();
            throw new SQLException("Simulated exception during statement execution");
        }
    };

    private static final ConnectionFunction successfulUpdate = conn -> {
        String updateSQL = "UPDATE users_directory.users SET email = ? WHERE username = ?";
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

    private void assertUserRow(ResultSet resultSet, String expectedUsername, String expectedEmail) throws SQLException {
        assertTrue(resultSet.next(), "Expected another row in the ResultSet");
        assertEquals(expectedUsername, resultSet.getString("username"));
        assertEquals(expectedEmail, resultSet.getString("email"));
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
    void should_insert_data_when_autocommit_on() throws Exception {
        assertDoesNotThrow(() -> DbUtils.executeStatements(successfulInsert));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            String selectSQL = "SELECT username, email FROM users_directory.users";
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@example.com");
                    assertUserRow(resultSet, "jane_doe", "jane.doe@example.com");
                }
            }
        }
    }

    @Test
    void should_insert_data_only_from_successful_operations_when_autocommit_on() throws Exception {
        assertThrows(RuntimeException.class, () -> DbUtils.executeStatements(failedInsert));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            String selectSQL = "SELECT username, email FROM users_directory.users";
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@example.com");
                    assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void should_insert_data_when_in_transaction() throws Exception {
        assertDoesNotThrow(() -> DbUtils.executeStatementsInTransaction(successfulInsert));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            String selectSQL = "SELECT username, email FROM users_directory.users";
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@example.com");
                    assertUserRow(resultSet, "jane_doe", "jane.doe@example.com");
                }
            }
        }
    }

    @Test
    void should_fail_to_insert_all_data_when_in_transaction() throws Exception {
        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransaction(failedInsert));

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            String selectSQL = "SELECT username, email FROM users_directory.users";
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void should_return_update_count_when_in_transaction() throws Exception {
        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            String insertSQL = "INSERT INTO users_directory.users (username, email) VALUES ('john_doe', 'john.doe@example.com')";
            try (PreparedStatement insertStatement = conn.prepareStatement(insertSQL)) {
                insertStatement.executeUpdate();
            }
        }

        Object updatedRowsCount = DbUtils.executeStatementsInTransactionWithResult(successfulUpdate);
        assertEquals(1, updatedRowsCount);

        try (Connection conn = DbUtils.getDataSource().getConnection()) {
            String selectSQL = "SELECT username, email FROM users_directory.users";
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, "john_doe", "john.doe@gmail.com");
                    assertFalse(resultSet.next());
                }
            }
        }
    }

}