package org.ldbcouncil.snb.driver.workloads.interactive;

import java.io.File;
import java.sql.SQLException;
import java.util.Iterator;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.csv.ParquetLoader;
import org.ldbcouncil.snb.driver.generator.EventStreamReader;
import org.ldbcouncil.snb.driver.util.Tuple2;

public class BatchedOperationStreamReader{

    private final ParquetLoader loader;

    public BatchedOperationStreamReader(
        ParquetLoader loader
    )
    {
        this.loader = loader;
    }

    public Tuple2<Long, Long> init(
        File operationFile,
        String viewName,
        String batchColumn
    ) throws WorkloadException, SQLException
    {
        loader.createViewOnParquetFile(
            operationFile.getAbsolutePath(),
            viewName
        );

        return loader.getBoundaryValues(batchColumn, viewName);
    }

    public Iterator<Operation> readBatchedOperationStream(
        EventStreamReader.EventDecoder<Operation> decoder,
        long offset,
        long batchSize,
        String viewName,
        String batchColumnName
    ) throws WorkloadException
    {
        Iterator<Operation> opStream;

        try
        {
            opStream = loader.getOperationStreamBatch(decoder, viewName, batchColumnName, offset, batchSize);
        }
        catch (SQLException e){
            e.printStackTrace();
            throw new WorkloadException("Error loading batched operation stream with view: " + viewName);
        }
        return opStream;
    }
}
