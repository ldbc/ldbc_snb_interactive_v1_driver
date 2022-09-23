package org.ldbcouncil.snb.driver.generator;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.ldbcouncil.snb.driver.Operation;

import static java.lang.String.format;

public class BufferedIterator implements Iterator<Operation> {
    
    private Iterator<Operation> currentOperationStream = Collections.emptyIterator();
    private final OperationStreamBuffer operationStreamBuffer;

    private boolean isEmpty = false;

    public BufferedIterator(
        OperationStreamBuffer operationStreamBuffer
    )
    {
        this.operationStreamBuffer = operationStreamBuffer;
    }

    public void init()
    {
        currentOperationStream = operationStreamBuffer.next();
    }

    @Override
    public boolean hasNext() 
    {
        if(!currentOperationStream.hasNext() && !isEmpty)
        {
            currentOperationStream = operationStreamBuffer.next();
            if (currentOperationStream == null)
            {
                currentOperationStream = Collections.emptyIterator();
                isEmpty = true;
            }
            if (!currentOperationStream.hasNext())
            {
                isEmpty = true;
            }
        }
        return currentOperationStream.hasNext();
    }

    @Override
    public Operation next()
    {
        if(hasNext())
        {
            return currentOperationStream.next();
        }
        else
        {
            return null;
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( format( "%s does not support remove()", getClass().getSimpleName() ) );
    }
}
