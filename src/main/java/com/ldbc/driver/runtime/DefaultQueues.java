package com.ldbc.driver.runtime;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

public class DefaultQueues
{
    public static <T> Queue<T> newNonBlocking()
    {
        return new ConcurrentLinkedQueue<>();
    }

    public static <T> BlockingQueue<T> newBlockingUnbounded()
    {
        return new LinkedTransferQueue<>();
    }

    public static final int DEFAULT_BOUND_1000 = 1000;

    public static <T> BlockingQueue<T> newBlockingBounded( int capacity )
    {
        return new LinkedBlockingQueue<>( capacity );
    }

    public static <T> BlockingQueue<T> newAlwaysBlockingBounded( int capacity )
    {
        return new AlwaysBlockingLinkedBlockingQueue<>( capacity );
    }

    /*
    turn offer() & add() into blocking calls (unless interrupted)
    */
    private static class AlwaysBlockingLinkedBlockingQueue<E> extends LinkedBlockingQueue<E>
    {
        public AlwaysBlockingLinkedBlockingQueue( int maxSize )
        {
            super( maxSize );
        }

        @Override
        public boolean offer( E e )
        {
            try
            {
                put( e );
                return true;
            }
            catch ( InterruptedException ie )
            {
                Thread.currentThread().interrupt();
            }
            return false;
        }

        @Override
        public boolean add( E e )
        {
            try
            {
                put( e );
                return true;
            }
            catch ( InterruptedException ie )
            {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }
}
