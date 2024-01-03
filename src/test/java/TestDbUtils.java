import com.liaverg.config.DataSourceProvider;
import com.liaverg.utilities.DbUtils;
import com.liaverg.utilities.DbUtils.ConnectionConsumer;
import com.liaverg.utilities.DbUtils.ConnectionFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class TestDbUtils {

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withInitScript("init.sql");

    private static DataSource dataSource;
    private static final int LEAK_DETECTION_THRESHOLD = 3000;

    @BeforeAll
    static void setUp() {
        DataSourceProvider dataSourceProvider = new DataSourceProvider(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword(),
                LEAK_DETECTION_THRESHOLD
        );
        new DbUtils(dataSourceProvider.getHikariProxyDataSource());
        dataSource = dataSourceProvider.getHikariDataSource();
    }

    @AfterEach
    void tearDown() throws Exception {
        truncateTable();
    }

    private void truncateTable() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            String truncateSQL = "TRUNCATE TABLE users_directory.users";
            try (PreparedStatement truncateStatement = conn.prepareStatement(truncateSQL)) {
                truncateStatement.executeUpdate();
            }
        }
    }

    private void insertTwoRecords(String username1, String email1,
                                  String username2, String email2) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            insertUser(conn, username1, email1);
            insertUser(conn, username2, email2);
        }
    }

    private void insertUser(Connection conn, String username, String email) throws SQLException {
        String insertSQL = "INSERT INTO users_directory.users (username, email) VALUES (?, ?)";
        try (PreparedStatement insertStatement = conn.prepareStatement(insertSQL)) {
            insertStatement.setString(1, username);
            insertStatement.setString(2, email);
            insertStatement.executeUpdate();
        }
    }

    private int updateUser(Connection conn, String username, String email) throws SQLException {
        String updateSQL = "UPDATE users_directory.users SET email = ? WHERE username = ?";
        try (PreparedStatement updateStatement = conn.prepareStatement(updateSQL)) {
            updateStatement.setString(1, email);
            updateStatement.setString(2, username);
            return updateStatement.executeUpdate();
        }
    }

    private void verifyNoRecordInTheDatabase() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            String selectSQL = "SELECT username, email FROM users_directory.users";
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertFalse(resultSet.next());
                }
            }
        }
    }

    private void verifyTwoRecordsInTheDatabase(String expectedUsername1, String expectedEmail1,
                                               String expectedUsername2, String expectedEmail2) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            String selectSQL = "SELECT username, email FROM users_directory.users";
            try (PreparedStatement selectStatement = conn.prepareStatement(selectSQL)) {
                try (ResultSet resultSet = selectStatement.executeQuery()) {
                    assertUserRow(resultSet, expectedUsername1, expectedEmail1);
                    assertUserRow(resultSet, expectedUsername2, expectedEmail2);
                    assertFalse(resultSet.next());
                }
            }
        }
    }

    private void assertUserRow(ResultSet resultSet, String expectedUsername, String expectedEmail) throws SQLException {
        assertTrue(resultSet.next(), "Expected another row in the ResultSet");
        assertEquals(expectedUsername, resultSet.getString("username"));
        assertEquals(expectedEmail, resultSet.getString("email"));
    }

    @Test
    @DisplayName("Successful Insert when Autocommit On")
    void should_insert_two_records_when_autocommit_on() throws Exception {
        ConnectionConsumer successfulInsert = conn -> {
            insertUser(conn, "john_doe", "john.doe@example.com");
            insertUser(conn, "jane_doe", "jane.doe@example.com");
        };

        assertDoesNotThrow(() -> DbUtils.executeStatements(successfulInsert));

        verifyTwoRecordsInTheDatabase("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
    }

    @Test
    @DisplayName("Successful Insert with Simulated Error when Autocommit On")
    void should_insert_two_records_with_simulated_error_when_autocommit_on() throws Exception {
        ConnectionConsumer insertWithSimulatedError = conn -> {
            insertUser(conn, "john_doe", "john.doe@example.com");
            insertUser(conn, "jane_doe", "jane.doe@example.com");
            throw new SQLException("Simulated exception during statement execution");
        };

        assertThrows(RuntimeException.class, () -> DbUtils.executeStatements(insertWithSimulatedError));

        verifyTwoRecordsInTheDatabase("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
    }

    @Test
    @DisplayName("Successful Insert in Transaction")
    void should_insert_two_records_when_in_transaction() throws Exception {
        ConnectionConsumer successfulInsert = conn -> {
            insertUser(conn, "john_doe", "john.doe@example.com");
            insertUser(conn, "jane_doe", "jane.doe@example.com");
        };

        assertDoesNotThrow(() -> DbUtils.executeStatementsInTransaction(successfulInsert));

        verifyTwoRecordsInTheDatabase("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
    }

    @Test
    @DisplayName("Failed Insert in Transaction")
    void should_fail_to_insert_records_when_in_transaction() throws Exception {
        ConnectionConsumer failedInsert = conn -> {
            insertUser(conn, "john_doe", "john.doe@example.com");
            insertUser(conn, "jane_doe", "jane.doe@example.com");
            throw new SQLException("Simulated exception during statement execution");
        };

        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransaction(failedInsert));

        verifyNoRecordInTheDatabase();
    }

    @Test
    @DisplayName("Successful Update in Transaction")
    void should_return_count_of_updated_records_when_in_transaction() throws Exception {
        insertTwoRecords("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
        ConnectionFunction successfulUpdate = conn -> {
            int updateCount = 0;
            updateCount += updateUser(conn, "john_doe", "john.doe@gmail.com");
            updateCount += updateUser(conn, "jane_doe", "jane.doe@gmail.com");
            return updateCount;
        };

        Object updatedRowsCount = DbUtils.executeStatementsInTransactionWithResult(successfulUpdate);
        assertEquals(2, updatedRowsCount);

        verifyTwoRecordsInTheDatabase("john_doe", "john.doe@gmail.com",
                "jane_doe", "jane.doe@gmail.com");
    }

    @Test
    @DisplayName("Failed Update in Transaction")
    void should_fail_to_update_when_in_transaction() throws Exception {
        insertTwoRecords("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
        ConnectionFunction failedUpdate = conn -> {
            updateUser(conn, "john_doe", "john.doe@gmail.com");
            updateUser(conn, "jane_doe", "jane.doe@gmail.com");
            throw new SQLException("Simulated exception during statement execution");
        };

        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransactionWithResult(failedUpdate));

        verifyTwoRecordsInTheDatabase("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
    }

    @Test
    @DisplayName("Successful Nested Insert")
    void should_insert_when_in_nested_transactions() throws Exception {
        ConnectionConsumer successfulNestedInsert = conn -> {
            DbUtils.executeStatementsInTransaction(connection ->
                    insertUser(connection, "john_doe", "john.doe@example.com"));
            insertUser(conn, "jane_doe", "jane.doe@example.com");
        };

        assertDoesNotThrow(() -> DbUtils.executeStatementsInTransaction(successfulNestedInsert));

        verifyTwoRecordsInTheDatabase("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
    }

    @Test
    @DisplayName("Failed Inner Insert in Nested Transactions")
    void should_fail_to_insert_when_inner_insert_fails_in_nested_transactions() throws Exception {
        ConnectionConsumer failedInnerInsert = conn -> {
            DbUtils.executeStatementsInTransaction(connection -> {
                insertUser(connection, "john_doe", "john.doe@example.com");
                throw new SQLException("Simulated exception during statement execution");
            });
            insertUser(conn, "jane_doe", "jane.doe@example.com");
        };

        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransaction(failedInnerInsert));

        verifyNoRecordInTheDatabase();
    }

    @Test
    @DisplayName("Failed Outer Insert in Nested Transactions")
    void should_fail_to_insert_when_outer_insert_fails_in_nested_transactions() throws Exception {
        ConnectionConsumer failedOuterInsert = conn -> {
            DbUtils.executeStatementsInTransaction(connection ->
                    insertUser(connection, "john_doe", "john.doe@example.com"));
            insertUser(conn, "jane_doe", "jane.doe@example.com");
            throw new SQLException("Simulated exception during statement execution");
        };
        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransaction(failedOuterInsert));

        verifyNoRecordInTheDatabase();
    }

    @Test
    @DisplayName("Successful Update in Nested Transactions")
    void should_return_count_of_updated_records_when_in_nested_transactions() throws Exception {
        insertTwoRecords("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
        ConnectionFunction successfulNestedUpdate = conn -> {
            int updateCount = 0;
            updateCount += DbUtils.executeStatementsInTransactionWithResult(connection ->
                    updateUser(connection, "john_doe", "john.doe@gmail.com"));
            updateCount += updateUser(conn, "jane_doe", "jane.doe@gmail.com");
            return updateCount;
        };

        Object updatedRowsCount = DbUtils.executeStatementsInTransactionWithResult(successfulNestedUpdate);
        assertEquals(2, updatedRowsCount);

        verifyTwoRecordsInTheDatabase("john_doe", "john.doe@gmail.com",
                "jane_doe", "jane.doe@gmail.com");
    }

    @Test
    @DisplayName("Failed Inner Update in Nested Transactions")
    void should_fail_to_update_when_inner_update_fails_in_nested_transactions() throws Exception {
        insertTwoRecords("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
        ConnectionFunction failedInnerUpdate = conn -> {
            DbUtils.executeStatementsInTransactionWithResult(connection -> {
                updateUser(connection, "john_doe", "john.doe@gmail.com");
                throw new SQLException("Simulated exception during statement execution");
            });
            return updateUser(conn, "jane_doe", "jane.doe@gmail.com");
        };

        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransactionWithResult(failedInnerUpdate));

        verifyTwoRecordsInTheDatabase("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
    }

    @Test
    @DisplayName("Failed Outer Update in Nested Transactions")
    void should_fail_to_update_when_outer_update_fails_in_nested_transactions() throws Exception {
        insertTwoRecords("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
        ConnectionFunction failedOuterUpdate = conn -> {
            DbUtils.executeStatementsInTransactionWithResult(connection ->
                    updateUser(connection, "john_doe", "john.doe@gmail.com"));
            updateUser(conn, "jane_doe", "jane.doe@gmail.com");
            throw new SQLException("Simulated exception during statement execution");
        };

        assertThrows(RuntimeException.class, () -> DbUtils.executeStatementsInTransactionWithResult(failedOuterUpdate));

        verifyTwoRecordsInTheDatabase("john_doe", "john.doe@example.com",
                "jane_doe", "jane.doe@example.com");
    }
}