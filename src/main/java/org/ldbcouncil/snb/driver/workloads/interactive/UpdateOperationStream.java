package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * 
 */
import java.io.File;
import java.sql.SQLException;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.generator.EventStreamReader;
import java.util.Iterator;

import org.ldbcouncil.snb.driver.csv.CsvLoader;

public class UpdateOperationStream {
    private final char columnDelimiter = '|';

    private final CsvLoader loader;

    /**
     * 
     * @param loader CsvLoader object.
     */
    public UpdateOperationStream(CsvLoader loader)
    {
        this.loader = loader;
    }

    /**
     * @param updateOperationFile
     * @param decoder
     * @return
     * @throws WorkloadException
     */
    public Iterator<Operation> readUpdateStream(
        File updateOperationFile,
        EventStreamReader.EventDecoder<Operation> decoder
        ) throws WorkloadException
    {
        Iterator<Operation> opStream;
        try 
        {
            opStream = loader.loadOperationStream(updateOperationFile.getAbsolutePath(), columnDelimiter, decoder);
        }
        catch (SQLException  e){
            throw new WorkloadException("Error creating txt loader");
        }

        return opStream;
    }
}
