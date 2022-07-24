package org.ldbcouncil.snb.driver.generator;

import java.util.Collections;
import java.util.Iterator;

import org.ldbcouncil.snb.driver.Operation;

import static java.lang.String.format;

public class BufferedIterator implements Iterator<Operation> {
    
    private Iterator<Operation> currentOperationStream = Collections.emptyIterator();
    private final OperationStreamBuffer operationStreamBuffer;

    public BufferedIterator(
        OperationStreamBuffer operationStreamBuffer
    )
    {
        this.operationStreamBuffer = operationStreamBuffer;
        hasNext();
    }

    @Override
    public boolean hasNext() 
    {
        if(!currentOperationStream.hasNext())
        {
            currentOperationStream = operationStreamBuffer.next();
            if (currentOperationStream == null)
            {
                return false;
            }
        }
        return currentOperationStream.hasNext();
    }

    @Override
    public Operation next()
    {
        return currentOperationStream.next();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }
}
