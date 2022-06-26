package org.ldbcouncil.snb.driver.workloads.interactive;

/**
 * ReadOperationStream.java
 * 
 * Class to read the operation stream txt files (substitution parameters)
 */
import java.io.File;
import java.sql.SQLException;
import java.util.Iterator;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.csv.CsvLoader;
import org.ldbcouncil.snb.driver.generator.GeneratorFactory;
import org.ldbcouncil.snb.driver.generator.EventStreamReader;

public class ReadOperationStream {
    
    private final char columnDelimiter = '|';

    private final GeneratorFactory gf;
    private final long workloadStartTimeAsMilli;
    private final CsvLoader loader;

    public ReadOperationStream(GeneratorFactory gf, long workloadStartTimeAsMilli, CsvLoader loader)
    {
        this.gf = gf;
        this.workloadStartTimeAsMilli = workloadStartTimeAsMilli;
        this.loader = loader;
    }

    public Iterator<Operation> readOperationStream(
        EventStreamReader.EventDecoder<Operation> decoder,
        long readOperationInterleaveAsMilli,
        File readOperationFile
    ) throws WorkloadException
    {
        Iterator<Operation> opStream;

        try {
            opStream = loader.loadOperationStream(readOperationFile.getAbsolutePath(), columnDelimiter, decoder);
        }
        catch (SQLException e){
            throw new WorkloadException("Error loading substitution parameters");
        }
        
        Iterator<Operation> operationStreamWithoutTimes = new QueryEventStreamReader(
            gf.repeating( opStream )
        );

        Iterator<Long> operationStartTimes =
            gf.incrementing( workloadStartTimeAsMilli + readOperationInterleaveAsMilli,
                    readOperationInterleaveAsMilli );

        return gf.assignStartTimes(
            operationStartTimes,
            operationStreamWithoutTimes
        );
    }
}
