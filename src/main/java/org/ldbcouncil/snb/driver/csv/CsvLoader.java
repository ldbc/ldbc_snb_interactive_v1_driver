package org.ldbcouncil.snb.driver.csv;

/**
 * CsvLoader.java
 * 
 * Class used to load the substitution parameters and update streams.
 * The substitution streams are loaded completely since they are approx ~15Mb
 * in size total. UpdateStreams are loaded in chunks because of the large 
 * size at higher scale factors (e.g. SF300).
 * 
 * The CSV is loaded using DuckDb's read_csv_auto function.
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
import org.ldbcouncil.snb.driver.generator.QueryEventStreamDecoder;
import org.ldbcouncil.snb.driver.generator.UpdateEventStreamDecoder;

import static java.lang.String.format;

public class CsvLoader {
    
    private final DuckDbConnectionState db;

    /**
     * Default constructor. 
     * @param db Object handling connections to the Database
     */
    public CsvLoader(DuckDbConnectionState db) {
        this.db = db;
    }

    /**
     * Loads operation stream. Returns an iterator with object-arrays containing the 
     * substitution parameters.
     * @param path Path to the operation stream
     * @param delimiter The delimiter used in the csv-file
     * @param decoder The decoder to use. Converts ResultSet to Object array.
     * @return
     * @throws WorkloadException When there is an error decoding the operation stream
     * @throws SQLException When an error occurs when fetching results.
     */
    public Iterator<Object[]> loadOperationStream(String path, char delimiter, QueryEventStreamDecoder.EventDecoder<Object[]> decoder) throws WorkloadException, SQLException
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

    /**
     * Loads update stream. Returns an iterator with object-arrays containing the 
     * @param path Path to the update stream
     * @param delimiter The delimiter used in the csv-file
     * @param decoder The decoder to use. Converts ResultSet to Operation.
     * @return
     * @throws WorkloadException When there is an error decoding the operation stream
     * @throws SQLException When an error occurs when fetching results.
     */
    public Iterator<Operation> loadOperationStream(String path, char delimiter, UpdateEventStreamDecoder.UpdateEventDecoder<Operation> decoder) throws WorkloadException, SQLException
    {
        Statement stmt = null;
        List<Operation> results = new ArrayList<>();
        try {
            Connection connection = db.getConnection();
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM read_csv_auto('" + path +"', delim = '" +delimiter+"', header=True);");
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
