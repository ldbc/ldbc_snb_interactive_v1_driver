package org.ldbcouncil.snb.driver.workloads.interactive;
import static java.lang.String.format;

/**
 * ReadOperationStream.java
 * 
 * Class to read the operation stream txt files (substitution parameters)
 */
import java.io.File;
import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.Iterator;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.csv.CsvLoader;
import org.ldbcouncil.snb.driver.generator.GeneratorFactory;
import org.ldbcouncil.snb.driver.generator.QueryEventStreamReader;

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
        Class evenStreamReaderClass,
        QueryEventStreamReader.EventDecoder<Operation> decoder,
        long readOperationInterleaveAsMilli,
        File readOperationFile
    ) throws WorkloadException
    {
        Iterator<Operation> opStream;

        try {
            opStream = loader.loadOperationStream(readOperationFile.getAbsolutePath(), columnDelimiter, decoder);
        }
        catch (SQLException  e){
            throw new WorkloadException("Error creating txt loader");
        }
        try {
            Constructor<Iterator<Operation>> ctor = evenStreamReaderClass.getConstructor(Iterator.class);
            Iterator<Operation> operationStreamWithoutTimes = ctor.newInstance(
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
        catch(Exception e)
        {
            throw new WorkloadException(format("Unable to instantiate event stream reader of class: %s", evenStreamReaderClass), e);
        }
    }
}
