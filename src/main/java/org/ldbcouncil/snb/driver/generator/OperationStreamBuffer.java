package org.ldbcouncil.snb.driver.generator;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.ldbcouncil.snb.driver.Operation;


public class OperationStreamBuffer implements Iterator<Iterator<Operation>>{
    
    private final BlockingQueue<Iterator<Operation>> blockingQueue;

    private boolean isEmpty = false;

    public OperationStreamBuffer(
        BlockingQueue<Iterator<Operation>> blockingQueue
    )
    {
        this.blockingQueue = blockingQueue;
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
