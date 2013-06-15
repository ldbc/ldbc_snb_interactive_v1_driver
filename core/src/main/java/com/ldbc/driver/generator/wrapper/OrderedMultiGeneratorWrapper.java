package com.ldbc.driver.generator.wrapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;

public class OrderedMultiGeneratorWrapper<T> extends Generator<T>
{
    private final List<GeneratorHead<T>> generatorHeads;
    private final Comparator<T> comparator;

    public static OrderedMultiGeneratorWrapper<Operation<?>> operationsByScheduledStartTime(
            Generator<Operation<?>>... operationGenerators )
    {
        return new OrderedMultiGeneratorWrapper<Operation<?>>( new Comparator<Operation<?>>()
        {
            @Override
            public int compare( Operation<?> o1, Operation<?> o2 )
            {
                return o1.getScheduledStartTimeNanoSeconds().compareTo( o2.getScheduledStartTimeNanoSeconds() );
            }
        }, operationGenerators );
    }

    public OrderedMultiGeneratorWrapper( Comparator<T> comparator, Generator<T>... operationGenerators )
    {
        super( null );
        this.comparator = comparator;
        this.generatorHeads = new ArrayList<OrderedMultiGeneratorWrapper.GeneratorHead<T>>();
        for ( Generator<T> generator : operationGenerators )
        {
            this.generatorHeads.add( new GeneratorHead<T>( generator ) );
        }
    }

    @Override
    protected T doNext() throws GeneratorException
    {
        GeneratorHead<T> minGeneratorHead = getMinGeneratorHead();
        return ( null == minGeneratorHead ) ? null : minGeneratorHead.removeHead();
    }

    private GeneratorHead<T> getMinGeneratorHead()
    {
        GeneratorHead<T> minGeneratorHead = null;
        Iterator<GeneratorHead<T>> generatorHeadsIterator = generatorHeads.iterator();

        while ( generatorHeadsIterator.hasNext() )
        {
            GeneratorHead<T> generatorHead = generatorHeadsIterator.next();
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
            GeneratorHead<T> generatorHead = generatorHeadsIterator.next();
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

    private static class GeneratorHead<T1>
    {
        private final Generator<T1> generator;
        private T1 head;

        public GeneratorHead( Generator<T1> generator )
        {
            this.generator = generator;
            this.head = ( this.generator.hasNext() ) ? this.generator.next() : null;
        }

        public T1 removeHead()
        {
            T1 oldHead = head;
            head = ( generator.hasNext() ) ? generator.next() : null;
            return oldHead;

        }

        public T1 inspectHead()
        {
            return head;
        }
    }
}
