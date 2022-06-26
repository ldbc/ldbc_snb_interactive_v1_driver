package org.ldbcouncil.snb.driver.workloads.interactive;

/**
 * ReadOperationStream.java
 * 
 * Class to read the operation streams:
 * - Substitution parameters csvs, used for Query1 to Query14
 * - Update streams, used for the insert and delete queries
 */
import java.io.File;
import java.sql.SQLException;
import java.util.Iterator;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.csv.CsvLoader;
import org.ldbcouncil.snb.driver.generator.EventStreamReader;

public class OperationStreamReader {
    
    private final char columnDelimiter = '|';

    private final CsvLoader loader;

    public OperationStreamReader(CsvLoader loader)
    {
        this.loader = loader;
    }

    public Iterator<Operation> readOperationStream(
        EventStreamReader.EventDecoder<Operation> decoder,
        File readOperationFile
    ) throws WorkloadException
    {
        Iterator<Operation> opStream;

        try
        {
            opStream = loader.loadOperationStream(readOperationFile.getAbsolutePath(), columnDelimiter, decoder);
        }
        catch (SQLException e){
            throw new WorkloadException("Error loading operation stream with path: " + readOperationFile.getAbsolutePath());
        }
        
        return opStream;

    }
}
