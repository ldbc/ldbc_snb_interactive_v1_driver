package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * 
 */
import java.io.File;
import java.sql.SQLException;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.generator.UpdateEventStreamDecoder;
import java.util.Iterator;

import org.ldbcouncil.snb.driver.csv.CsvLoader;

public class UpdateOperationStream {
    private final String columnDelimiter = "\r\n";

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
     * @param batchSize
     * @param decoder
     * @return
     * @throws WorkloadException
     */
    public Iterator<Operation> readUpdateStream(
        File updateOperationFile,
        UpdateEventStreamDecoder.UpdateEventDecoder<Operation> decoder
        ) throws WorkloadException
    {
        Iterator<Operation> opStream;
        try {
            opStream = loader.loadOperationStream(updateOperationFile.getAbsolutePath(), columnDelimiter, decoder);
        }
        catch (SQLException  e){
            throw new WorkloadException("Error creating txt loader");
        }

        return opStream;
    }
}
