import com.liaverg.DbUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;


public class TestDbUtils {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/testdb";
    private static final String USER = "postgres";
    private static final String PASSWORD = "root";

    private static DbUtils.ConnectionConsumer successfulInsertData = conn -> {
        String insertSQL = "INSERT INTO users_directory.users (username, email) VALUES (?, ?)";
        try (PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {
            insertStmt.setString(1, "john_doe");
            insertStmt.setString(2, "john.doe@example.com");
            insertStmt.executeUpdate();
        }
    };

    private static DbUtils.ConnectionConsumer failedInsertData = conn -> {
        String insertSQL = "INSERT users_directory.users (username, email) VALUES (?, ?)";
        try (PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {
            insertStmt.setString(1, "john_doe");
            insertStmt.setString(2, "john.doe@example.com");
            insertStmt.executeUpdate();
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
    void testExecuteStatements_InvalidStatements(){
        RuntimeException thrownException = assertThrows(RuntimeException.class, () -> DbUtils.executeStatements(failedInsertData));
        assertEquals("error during statement execution", thrownException.getMessage());
    }
}
