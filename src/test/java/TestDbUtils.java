import com.liaverg.DbUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;


public class TestDbUtils {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/testdb";
    private static final String USER = "postgres";
    private static final String PASSWORD = "root";

    private static final DbUtils.ConnectionConsumer successfulInsertData = conn -> {
        String insertSQL = "INSERT INTO users_directory.users (username, email) VALUES (?, ?)";
        try (PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {
            insertStmt.setString(1, "john_doe");
            insertStmt.setString(2, "john.doe@example.com");
            insertStmt.executeUpdate();
            insertStmt.setString(1, "jane_doe");
            insertStmt.setString(2, "jane.doe@example.com");
            insertStmt.executeUpdate();
        }
    };

    private static final DbUtils.ConnectionConsumer failedInsertData = conn -> {
        String insertSQL = "INSERT INTO users_directory.users (username, email) VALUES (?, ?)";
        try (PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {
            insertStmt.setString(1, "john_doe");
            insertStmt.setString(2, "john.doe@example.com");
            insertStmt.executeUpdate();
            insertStmt.setString(1, "jane_doe");
            insertStmt.setString(3, "jane.doe@example.com");
            insertStmt.executeUpdate();
        }
    };

    private static final DbUtils.ConnectionFunction selectData = conn -> {
        String selectSQL = "SELECT username, email FROM users_directory.users";
        try (PreparedStatement selectStmt = conn.prepareStatement(selectSQL)) {
            try (ResultSet resultSet = selectStmt.executeQuery()) {
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
    private static final DbUtils.ConnectionFunction failedSelectData = conn -> {
        String selectSQL = "SELECT username, email FROM users_directory.users";
        try (PreparedStatement selectStmt = conn.prepareStatement(selectSQL)) {
            try (ResultSet resultSet = selectStmt.executeQuery()) {
                HashSet<String []> usersSet= new HashSet<>();
                while (resultSet.next()) {
                    String username = resultSet.getString("");
                    String email = resultSet.getString("email");
                    usersSet.add(new String[]{username, email});
                }
                return usersSet;
            }
        }
    };

    @BeforeAll
    public static void setUp() {
        PGSimpleDataSource pg_dataSource = new PGSimpleDataSource();
        pg_dataSource.setURL(DB_URL);
        pg_dataSource.setUser(USER);
        pg_dataSource.setPassword(PASSWORD);
        DataSource dataSource = pg_dataSource;
        DbUtils dbUtils = new DbUtils(dataSource);
    }

    @Test
    void testExecuteStatements_HappyDayScenario(){
        assertDoesNotThrow (() -> DbUtils.executeStatements(successfulInsertData));
    }

    @Test
    void testExecuteStatements_InvalidStatement(){
        RuntimeException thrownException = assertThrows(RuntimeException.class, () ->
                                                        DbUtils.executeStatements(failedInsertData));
        assertEquals("error during statement execution", thrownException.getMessage());
    }

    @Test
    void testExecuteStatementsInTransaction_HappyDayScenario(){
        assertDoesNotThrow (() -> DbUtils.executeStatementsInTransaction(successfulInsertData));
    }

    @Test
    void testExecuteStatementsInTransaction_InvalidStatement(){
        RuntimeException thrownException = assertThrows(RuntimeException.class, () ->
                                                        DbUtils.executeStatementsInTransaction(failedInsertData));
        assertEquals("error during statement execution", thrownException.getMessage());
    }

    @Test
    void testExecuteStatementsInTransactionWithResult_HappyDayScenario(){
        assertDoesNotThrow (() -> DbUtils.executeStatementsInTransactionWithResult(selectData));
    }

    @Test
    void testExecuteStatementsInTransactionWithResult_InvalidStatement(){
        RuntimeException thrownException = assertThrows(RuntimeException.class, () ->
                                                        DbUtils.executeStatementsInTransactionWithResult(failedSelectData));
        assertEquals("error during statement execution", thrownException.getMessage());
    }
}