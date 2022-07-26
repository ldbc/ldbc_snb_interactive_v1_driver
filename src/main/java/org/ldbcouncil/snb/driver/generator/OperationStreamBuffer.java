package org.ldbcouncil.snb.driver.generator;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.csv.ParquetLoader;
import org.ldbcouncil.snb.driver.workloads.interactive.RunnableOperationStreamBatchLoader;


public class OperationStreamBuffer implements Iterator<Iterator<Operation>>{
    
    private final BlockingQueue<Iterator<Operation>> blockingQueue;
    private final ParquetLoader loader;
    private final GeneratorFactory gf;
    private final int numThreads;
    private final long batchSize;
    private final Set<Class<? extends Operation>> enabledUpdateOperationTypes;
    private final File updatesDir;

    private boolean isEmpty = false;

    private Thread runnableBatchLoader;

    public OperationStreamBuffer(
        ParquetLoader loader,
        File updatesDir,
        GeneratorFactory gf,
        long batchSize,
        int numThreads,
        Set<Class<? extends Operation>> enabledUpdateOperationTypes
    )
    {
        this.blockingQueue = new LinkedBlockingDeque<>(numThreads);
        this.loader = loader;
        this.updatesDir = updatesDir;
        this.gf = gf;
        this.enabledUpdateOperationTypes = enabledUpdateOperationTypes;
        this.batchSize = batchSize;
        this.numThreads = numThreads;
    }

    // Fills up the buffer
    public void init()
    {
        Runnable batch = new RunnableOperationStreamBatchLoader(
            loader,
            gf,
            updatesDir,
            blockingQueue,
            enabledUpdateOperationTypes,
            batchSize,
            numThreads
        );
        // Start the thread
        runnableBatchLoader = new Thread(batch);
        runnableBatchLoader.start();
    }

    @Override
    public Iterator<Operation> next()
    {
        if (!isEmpty)
        {
            Iterator<Operation> opStream = null;
            try {
                opStream = blockingQueue.poll(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (opStream == null)
            {
                isEmpty = true;
                return Collections.emptyIterator();
            }
            return opStream;
        }
        else
        {
            return null;
        }

    }

    @Override
    public boolean hasNext()
    {
        return !blockingQueue.isEmpty();
    }
}
