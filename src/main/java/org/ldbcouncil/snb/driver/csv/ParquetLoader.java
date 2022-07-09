package org.ldbcouncil.snb.driver.csv;

/**
 * ParquetLoader.java
 * 
 * Class to read Parquet files for operation streams.
 */
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.generator.EventStreamReader;

import static java.lang.String.format;

public class ParquetLoader {
    
    private final DuckDbConnectionState db;

    public ParquetLoader(DuckDbConnectionState db) throws SQLException{
        this.db = db;
    }

    public Iterator<Operation> loadOperationStream(String path, EventStreamReader.EventDecoder<Operation> decoder) throws WorkloadException, SQLException
    {
        Statement stmt = null;
        List<Operation> results = new ArrayList<>();
        try {
            Connection connection = db.getConnection();
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM read_parquet('" + path + "');");
            while (rs.next()) {
                Operation obj = decoder.decodeEvent(rs);
                results.add(obj);
            }
            rs.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
            throw new WorkloadException(format("Error loading substitution parameters into temporary database: %s", path), e);
        }
        finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return results.iterator();
    }
}
