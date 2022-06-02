package org.ldbcouncil.snb.driver.csv;
/**
 * DuckDbManager.java
 * 
 * Handles the connection with DuckDb.
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.io.Closeable;


public class DuckDbConnectionState implements Closeable {

    private final Connection connection;
    
    public DuckDbConnectionState() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
    }

    /**
     * Get connection to DuckDb instance
     * @return Connection object.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Close connection 
     */
    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
