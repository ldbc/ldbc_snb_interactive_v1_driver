package com.ldbc.driver.generator.wrapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;

public class OrderedMultiGeneratorWrapper<GENERATE_TYPE> extends Generator<GENERATE_TYPE>
{
    private static final int DEFAULT_LOOK_AHEAD_DISTANCE = 1;
    private final List<GeneratorHead<GENERATE_TYPE>> generatorHeads;
    private final Comparator<GENERATE_TYPE> comparator;

    // TODO move to GeneratorUtils?
    public static OrderedMultiGeneratorWrapper<Operation<?>> operationsByScheduledStartTime( int lookaheadDistance,
            Generator<Operation<?>>... operationGenerators )
    {
        return new OrderedMultiGeneratorWrapper<Operation<?>>( new Comparator<Operation<?>>()
        {
            @Override
            public int compare( Operation<?> o1, Operation<?> o2 )
            {
                return o1.scheduledStartTime().compareTo( o2.scheduledStartTime() );
            }
        }, lookaheadDistance, operationGenerators );
    }

    public OrderedMultiGeneratorWrapper( Comparator<GENERATE_TYPE> comparator, Generator<GENERATE_TYPE>... generators )
    {
        this( comparator, DEFAULT_LOOK_AHEAD_DISTANCE, generators );
    }

    public OrderedMultiGeneratorWrapper( Comparator<GENERATE_TYPE> comparator, int lookaheadDistance,
            Generator<GENERATE_TYPE>... generators )
    {
        this.comparator = comparator;
        if ( DEFAULT_LOOK_AHEAD_DISTANCE == lookaheadDistance )
        {
            this.generatorHeads = buildSimpleGeneratorHeads( generators );
        }
        else
        {
            this.generatorHeads = buildLookaheadGeneratorHeads( comparator, lookaheadDistance, generators );
        }
    }

    private static <T1> List<GeneratorHead<T1>> buildSimpleGeneratorHeads( Generator<T1>... generators )
    {
        List<GeneratorHead<T1>> heads = new ArrayList<GeneratorHead<T1>>();
        for ( Generator<T1> generator : generators )
        {
            heads.add( new SimpleGeneratorHead<T1>( generator ) );
        }
        return heads;
    }

    private static <T1> List<GeneratorHead<T1>> buildLookaheadGeneratorHeads( Comparator<T1> c, int distance,
            Generator<T1>... generators )
    {
        List<GeneratorHead<T1>> heads = new ArrayList<GeneratorHead<T1>>();
        for ( Generator<T1> generator : generators )
        {
            heads.add( new LookaheadGeneratorHead<T1>( generator, c, distance ) );
        }
        return heads;
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException
    {
        GeneratorHead<GENERATE_TYPE> minGeneratorHead = getMinGeneratorHead();
        return ( null == minGeneratorHead ) ? null : minGeneratorHead.removeHead();
    }

    private GeneratorHead<GENERATE_TYPE> getMinGeneratorHead()
    {
        GeneratorHead<GENERATE_TYPE> minGeneratorHead = null;
        Iterator<GeneratorHead<GENERATE_TYPE>> generatorHeadsIterator = generatorHeads.iterator();

        while ( generatorHeadsIterator.hasNext() )
        {
            GeneratorHead<GENERATE_TYPE> generatorHead = generatorHeadsIterator.next();
            if ( null == generatorHead.inspectHead() )
            {
                generatorHeadsIterator.remove();
                continue;
            }
            minGeneratorHead = generatorHead;
            break;
        }

        while ( generatorHeadsIterator.hasNext() )
        {
            GeneratorHead<GENERATE_TYPE> generatorHead = generatorHeadsIterator.next();
            if ( null == generatorHead.inspectHead() )
            {
                generatorHeadsIterator.remove();
                continue;
            }
            if ( comparator.compare( generatorHead.inspectHead(), minGeneratorHead.inspectHead() ) < 0 )
            {
                minGeneratorHead = generatorHead;
            }
        }
        return minGeneratorHead;
    }

    private static interface GeneratorHead<T1>
    {
        public T1 removeHead();

        public T1 inspectHead();
    }

    private static class SimpleGeneratorHead<T1> implements GeneratorHead<T1>
    {
        private final Generator<T1> generator;
        private T1 head;

        public SimpleGeneratorHead( Generator<T1> generator )
        {
            this.generator = generator;
            this.head = ( this.generator.hasNext() ) ? this.generator.next() : null;
        }

        @Override
        public T1 removeHead()
        {
            T1 oldHead = head;
            head = ( generator.hasNext() ) ? generator.next() : null;
            return oldHead;

        }

        @Override
        public T1 inspectHead()
        {
            return head;
        }
    }

    private static class LookaheadGeneratorHead<T1> implements GeneratorHead<T1>
    {
        private final Generator<T1> generator;
        private final Comparator<T1> comparator;
        private final int lookaheadDistance;
        private List<T1> lookaheadBuffer;
        private T1 head;

        public LookaheadGeneratorHead( Generator<T1> generator, Comparator<T1> comparator, int lookaheadDistance )
        {
            this.generator = generator;
            this.comparator = comparator;
            this.lookaheadDistance = lookaheadDistance;
            this.lookaheadBuffer = new ArrayList<T1>();
            fillLookaheadBuffer();
            this.head = getMinFromLookaheadBuffer();
        }

        @Override
        public T1 removeHead()
        {
            T1 oldHead = head;
            fillLookaheadBuffer();
            head = getMinFromLookaheadBuffer();
            return oldHead;

        }

        @Override
        public T1 inspectHead()
        {
            return head;
        }

        private T1 getMinFromLookaheadBuffer()
        {
            Iterator<T1> lookaheadBufferIterator = lookaheadBuffer.iterator();
            if ( false == lookaheadBufferIterator.hasNext() )
            {
                return null;
            }
            int index = 0;
            int minIndex = index;
            T1 min = lookaheadBufferIterator.next();
            while ( lookaheadBufferIterator.hasNext() )
            {
                index++;
                T1 next = lookaheadBufferIterator.next();
                if ( comparator.compare( next, min ) < 0 )
                {
                    minIndex = index;
                    min = next;
                }
            }
            lookaheadBuffer.remove( minIndex );
            return min;
        }

        private void fillLookaheadBuffer()
        {
            while ( generator.hasNext() )
            {
                if ( lookaheadBuffer.size() >= lookaheadDistance ) break;
                lookaheadBuffer.add( generator.next() );
            }
        }
    }
}
