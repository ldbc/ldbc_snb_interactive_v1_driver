package org.ldbcouncil.snb.driver.workloads.interactive;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.csv.ParquetLoader;
import org.ldbcouncil.snb.driver.generator.EventStreamReader;
import org.ldbcouncil.snb.driver.generator.GeneratorFactory;
import org.ldbcouncil.snb.driver.util.Tuple2;

public class RunnableOperationStreamBatchLoader extends Thread {
    
    private final ParquetLoader loader;
    private final long batchSize;
    private final GeneratorFactory gf;
    private final File updatesDir;
    private final Set<Class<? extends Operation>> enabledUpdateOperationTypes;
    private final BlockingQueue<Iterator<Operation>> blockingQueue;

    public RunnableOperationStreamBatchLoader (
        ParquetLoader loader,
        GeneratorFactory gf,
        File updatesDir,
        BlockingQueue<Iterator<Operation>> blockingQueue,
        Set<Class<? extends Operation>> enabledUpdateOperationTypes,
        long batchSize
    )
    {
        this.loader = loader;
        this.gf = gf;
        this.updatesDir = updatesDir;
        this.blockingQueue = blockingQueue;
        this.enabledUpdateOperationTypes = enabledUpdateOperationTypes;
        this.batchSize = batchSize;
    }

    /**
     * Loads the operation streams into the LinkedBlockingDeque
     */
    @Override
    public void run()
    {
        BatchedOperationStreamReader updateOperationStream = new BatchedOperationStreamReader(loader);
        Map<Class<? extends Operation>, String> classToPathMap = LdbcSnbInteractiveWorkloadConfiguration.getUpdateStreamClassToPathMapping();
        Map<Class<? extends Operation>, String> classToBatchColumn = LdbcSnbInteractiveWorkloadConfiguration.getUpdateStreamClassToDateColumn();
        long offset = Long.MAX_VALUE;
        Map<Class<? extends Operation>, Long> classToLastValue = new HashMap<>();
        try {
            for (Class<? extends Operation> enabledClass : enabledUpdateOperationTypes) {
                String filename = classToPathMap.get(enabledClass);
                String batchColumn = classToBatchColumn.get(enabledClass);
                String viewName = enabledClass.getSimpleName();
                // Initialize the batch reader to set the view in DuckDB on the parquet file
                Tuple2<Long, Long> boundaries = updateOperationStream.init(
                    new File( updatesDir, filename),
                    viewName,
                    batchColumn
                );
    
                classToLastValue.put(enabledClass, boundaries._2());

                if ( boundaries._1() < offset )
                {
                    offset = boundaries._1();
                }
            }

            // Loop until interrupt or no operations left to load
            while (!Thread.interrupted()) {
                List<Iterator<Operation>> newBatch = loadNextBatch(
                    updateOperationStream,
                    offset,
                    classToLastValue
                );
                if (newBatch.isEmpty())
                {
                    // No new operations, stream empty.
                    return;
                }
                for (Iterator<Operation> iterator : newBatch) {
                    // Waits for a free slot.
                    blockingQueue.put(iterator);
                }
                offset = offset + batchSize;
            }
        }
        catch (WorkloadException | SQLException ew){
            ew.printStackTrace();
            Thread.currentThread().interrupt();
        }
        catch ( InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Fetches the next batch of operation streams
     * @param updateOperationStream
     * @return
     * @throws SQLException
     * @throws WorkloadException
     */
    private List<Iterator<Operation>> loadNextBatch(
        BatchedOperationStreamReader updateOperationStream,
        long offset,
        Map<Class<? extends Operation>, Long> classtoEndValue
    ) throws SQLException, WorkloadException
    {
        Map<Class<? extends Operation>, String> classToBatchColumn = LdbcSnbInteractiveWorkloadConfiguration.getUpdateStreamClassToDateColumn();
        ArrayList<Iterator<Operation>> listOfBatchedOperationStreams = new ArrayList<>();

        Map<Class<? extends Operation>, EventStreamReader.EventDecoder<Operation>> decoders = UpdateEventStreamReader.getDecoders();
        for (Class<? extends Operation> enabledClass : enabledUpdateOperationTypes) {
            String batchColumn = classToBatchColumn.get(enabledClass);
            String viewName = enabledClass.getSimpleName();
            if (offset <= classtoEndValue.get(enabledClass)) {
                Iterator<Operation> operationStream = updateOperationStream.readBatchedOperationStream(
                    decoders.get(enabledClass),
                    offset,
                    batchSize,
                    viewName,
                    batchColumn
                );
                // Update offset
                // Only put non-empty iterators in the list to merge
                if (operationStream.hasNext()){
                    listOfBatchedOperationStreams.add(operationStream);
                }
            }
        }
        // If empty, it means there is nothing more to load.
        if (listOfBatchedOperationStreams.isEmpty())
        {
            return listOfBatchedOperationStreams;
        }
        // Merge the operation streams and sort them by timestamp
        List<Iterator<Operation>> listOfMergedAndSplittedOperationStreams = new ArrayList<>();
        Iterator<Operation> mergedUpdateStreams = Collections.<Operation>emptyIterator();
        for (Iterator<Operation> updateStream : listOfBatchedOperationStreams) {
            mergedUpdateStreams = gf.mergeSortOperationsByTimeStamp(mergedUpdateStreams,  updateStream);
        }

        listOfMergedAndSplittedOperationStreams.add(mergedUpdateStreams);
     
        return listOfMergedAndSplittedOperationStreams;
    }
}
