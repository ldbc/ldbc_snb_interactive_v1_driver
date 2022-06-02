package org.ldbcouncil.snb.driver.csv;

/**
 * CsvLoader.java
 * 
 * Class used to load the substitution parameters.
 */
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.generator.QueryEventStreamReader;

import static java.lang.String.format;

public class CsvLoader {
    
    private final DuckDbConnectionState db;

    public CsvLoader(DuckDbConnectionState db) throws SQLException{
        this.db = db;
    }

    public Iterator<Object[]> loadOperationStream(String path, char delimiter, QueryEventStreamReader.EventDecoder<Object[]> decoder) throws WorkloadException, SQLException
    {
        Statement stmt = null;
        List<Object[]> results = new ArrayList<>();
        try {
            Connection connection = db.getConnection();
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM read_csv_auto('" + path +"', delim = '" +delimiter+"', header=True);");
            while (rs.next()) {
                Object[] obj = decoder.decodeEvent(rs);
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
