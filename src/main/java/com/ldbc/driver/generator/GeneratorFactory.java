package com.ldbc.driver.generator;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.util.Function0;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.Function2;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple2;
import com.ldbc.driver.util.Tuple3;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

public class GeneratorFactory
{

    private final RandomDataGeneratorFactory randomDataGeneratorFactory;

    public GeneratorFactory( RandomDataGeneratorFactory randomDataGeneratorFactory )
    {
        this.randomDataGeneratorFactory = randomDataGeneratorFactory;
    }

    private RandomDataGenerator randomDataGenerator = null;

    // Every returned generator (that takes a RandomDataGenerator as input) will use a different RandomDataGenerator
    // UPDATE (2014/08/11): for performance (random data generator instantiation seems to be expensive), one
    // generator shared among all generators
    RandomDataGenerator getRandom()
    {
        if ( null == randomDataGenerator )
        { randomDataGenerator = randomDataGeneratorFactory.newRandom(); }
        return randomDataGenerator;
    }

    /*
     * ----------------------------------------------------------------------------------------------------
     * ---------------------------------------- UTILS -----------------------------------------------------
     * ----------------------------------------------------------------------------------------------------
     */

    public enum OperationStreamComparisonResultType
    {
        PASS,
        FAIL_ONE_STREAM_IS_EMPTY,
        FAIL_STREAMS_HAVE_DIFFERENT_LENGTH,
        FAIL_ONE_OPERATION_IS_NULL,
        FAIL_OPERATIONS_NOT_EQUAL,
        FAIL_ONE_START_TIME_IS_NULL,
        FAIL_ONE_DEPENDENCY_TIME_IS_NULL,
        FAIL_START_TIMES_NOT_EQUAL,
        FAIL_TIME_STAMPS_NOT_EQUAL,
        FAIL_DEPENDENCY_TIME_STAMPS_NOT_EQUAL
    }

    public class OperationStreamComparisonResult
    {
        private final String errorMessage;
        private final OperationStreamComparisonResultType result;

        public OperationStreamComparisonResult( String errorMessage, OperationStreamComparisonResultType result )
        {
            this.errorMessage = errorMessage;
            this.result = result;
        }

        public String errorMessage()
        {
            return errorMessage;
        }

        public OperationStreamComparisonResultType resultType()
        {
            return result;
        }
    }

    /**
     * Compare operation streams by stream lengths and equality of the operations they contain
     *
     * @param operationStream1
     * @param operationStream2
     * @param compareTimes
     * @return
     */
    // TODO move into a separate class, test that class separately, use that class here
    // TODO add check for timeStamp()
    public OperationStreamComparisonResult compareOperationStreams( Iterator<Operation> operationStream1,
            Iterator<Operation> operationStream2,
            boolean compareTimes )
    {
        int operationNumber = 0;
        if ( operationStream1.hasNext() != operationStream2.hasNext() )
        {
            return new OperationStreamComparisonResult( "",
                    OperationStreamComparisonResultType.FAIL_ONE_STREAM_IS_EMPTY );
        }

        while ( operationStream1.hasNext() )
        {
            operationNumber++;
            if ( false == operationStream2.hasNext() )
            {
                return new OperationStreamComparisonResult(
                        format( "operation %s\nstream 2 is shorter", operationNumber ),
                        OperationStreamComparisonResultType.FAIL_STREAMS_HAVE_DIFFERENT_LENGTH );
            }
            Operation next1 = operationStream1.next();
            Operation next2 = operationStream2.next();
            if ( null == next1 && null == next2 )
            { continue; }
            if ( null == next1 || null == next2 )
            {
                return new OperationStreamComparisonResult(
                        format( "operation %s\none operation is null\nstream 1: %s\nstream 2: %s",
                                operationNumber, next1, next2 ),
                        OperationStreamComparisonResultType.FAIL_ONE_OPERATION_IS_NULL );
            }

            else if ( false == next1.equals( next2 ) )
            {
                return new OperationStreamComparisonResult(
                        format( "operation %s\noperations not equal\nstream 1: %s\nstream 2: %s",
                                operationNumber, next1, next2 ),
                        OperationStreamComparisonResultType.FAIL_OPERATIONS_NOT_EQUAL );
            }
            if ( compareTimes )
            {
                long scheduledStartTimeAsMilli1 = next1.scheduledStartTimeAsMilli();
                long scheduledStartTimeAsMilli2 = next2.scheduledStartTimeAsMilli();
                if ( -1 == scheduledStartTimeAsMilli1 && -1 == scheduledStartTimeAsMilli2 )
                {
                    // do nothing
                }
                else if ( -1 == scheduledStartTimeAsMilli1 || -1 == scheduledStartTimeAsMilli2 )
                {
                    return new OperationStreamComparisonResult(
                            format( "operation %s\none start time is null\nstream 1: %s\nstream 2: %s",
                                    scheduledStartTimeAsMilli1, scheduledStartTimeAsMilli2, operationNumber ),
                            OperationStreamComparisonResultType.FAIL_ONE_START_TIME_IS_NULL );
                }
                else if ( scheduledStartTimeAsMilli1 != scheduledStartTimeAsMilli2 )
                {
                    return new OperationStreamComparisonResult(
                            format( "operation %s\nstart times not equal\nstream 1: %s\nstream 2: %s",
                                    operationNumber, scheduledStartTimeAsMilli1, scheduledStartTimeAsMilli2 ),
                            OperationStreamComparisonResultType.FAIL_START_TIMES_NOT_EQUAL );
                }
                long timeStamp1 = next1.timeStamp();
                long timeStamp2 = next2.timeStamp();
                if ( -1 == timeStamp1 && -1 == timeStamp2 )
                {
                    // do nothing
                }
                else if ( -1 == timeStamp1 || -1 == timeStamp2 )
                {
                    return new OperationStreamComparisonResult(
                            format( "operation %s\none time stamp is null\nstream 1: %s\nstream 2: %s",
                                    timeStamp1, timeStamp2, operationNumber ),
                            OperationStreamComparisonResultType.FAIL_ONE_START_TIME_IS_NULL );
                }
                else if ( timeStamp1 != timeStamp2 )
                {
                    return new OperationStreamComparisonResult(
                            format( "operation %s\ntime stamps not equal\nstream 1: %s\nstream 2: %s",
                                    operationNumber, timeStamp1, timeStamp2 ),
                            OperationStreamComparisonResultType.FAIL_TIME_STAMPS_NOT_EQUAL );
                }
                long dependencyTimeStamp1 = next1.dependencyTimeStamp();
                long dependencyTimeStamp2 = next2.dependencyTimeStamp();
                if ( -1 == dependencyTimeStamp1 && -1 == dependencyTimeStamp2 )
                {
                    // do nothing
                }
                else if ( -1 == dependencyTimeStamp1 || -1 == dependencyTimeStamp2 )
                {
                    return new OperationStreamComparisonResult(
                            format( "operation %s\none dependency time is null\nstream1: %s\nstream2: %s",
                                    dependencyTimeStamp1, dependencyTimeStamp2, operationNumber ),
                            OperationStreamComparisonResultType.FAIL_ONE_DEPENDENCY_TIME_IS_NULL );
                }
                else if ( dependencyTimeStamp1 != dependencyTimeStamp2 )
                {
                    return new OperationStreamComparisonResult(
                            format( "operation %s\ndependency times not equal\nstream 1: %s\nstream 2: %s",
                                    operationNumber, dependencyTimeStamp1, dependencyTimeStamp2 ),
                            OperationStreamComparisonResultType.FAIL_DEPENDENCY_TIME_STAMPS_NOT_EQUAL );
                }
            }
        }
        if ( operationStream2.hasNext() )
        {
            return new OperationStreamComparisonResult(
                    format( "operation %s\nstream 1 is shorter", operationNumber ),
                    OperationStreamComparisonResultType.FAIL_STREAMS_HAVE_DIFFERENT_LENGTH );
        }
        return new OperationStreamComparisonResult( "", OperationStreamComparisonResultType.PASS );
    }

    public <T> void consume( Iterator<T> generator, long count )
    {
        for ( long consumed = 0; generator.hasNext() && consumed < count; consumed++ )
        {
            generator.next();
        }
    }
    /*
     * ----------------------------------------------------------------------------------------------------
     * ---------------------------------------- DECORATORS ------------------------------------------------
     * ----------------------------------------------------------------------------------------------------
     */

    /**
     * Wraps any number generator and keeps track of the minimum and maximum numbers returned by that generator.
     *
     * @param generator
     * @param initialMin
     * @param initialMax
     * @param <T>
     * @return
     */
    public <T extends Number> MinMaxGenerator<T> minMaxGenerator( Iterator<T> generator, T initialMin, T initialMax )
    {
        return new MinMaxGenerator<T>( generator, initialMin, initialMax );
    }

    /*
     * ----------------------------------------------------------------------------------------------------
     * ---------------------------------------- GENERATORS ------------------------------------------------
     * ----------------------------------------------------------------------------------------------------
     */

    public <T1> Iterator<T1> includeOnly( Iterator<T1> generator, T1... includedItems )
    {
        return includeOnly( generator, new IncludeOnlyPredicate<>( includedItems ) );
    }

    public <T1> Iterator<T1> includeOnly( Iterator<T1> generator, Predicate<T1> isIncludedPredicate )
    {
        return Iterators.filter( generator, isIncludedPredicate );
    }

    private class IncludeOnlyPredicate<T1> implements Predicate<T1>
    {
        private final Set<T1> includedItems;

        private IncludeOnlyPredicate( T1... includedItems )
        {
            this.includedItems = new HashSet<>( Arrays.asList( includedItems ) );
        }

        @Override
        public boolean apply( T1 input )
        {
            return true == includedItems.contains( input );
        }
    }

    public <T1> Iterator<T1> excludeAll( Iterator<T1> generator, T1... excludedItems )
    {
        return excludeAll( generator, new ExcludeAllPredicate<>( excludedItems ) );
    }

    public <T1> Iterator<T1> excludeAll( Iterator<T1> generator, Predicate<T1> isExcludedPredicate )
    {
        return Iterators.filter( generator, isExcludedPredicate );
    }

    private class ExcludeAllPredicate<T1> implements Predicate<T1>
    {
        private final Set<T1> excludedItems;

        private ExcludeAllPredicate( T1... excludedItems )
        {
            this.excludedItems = new HashSet<>( Arrays.asList( excludedItems ) );
        }

        @Override
        public boolean apply( T1 input )
        {
            return false == excludedItems.contains( input );
        }
    }

    /**
     * Maps/transforms one iterator into another, using input function to perform the transformation on each individual
     * element.
     * Returned iterator will have the same length input iterator.
     *
     * @param original
     * @param fun
     * @param <IN>
     * @param <OUT>
     * @return
     */
    public <IN, OUT> Iterator<OUT> map( Iterator<IN> original, Function1<IN,OUT,RuntimeException> fun )
    {
        return new MappingGenerator<>( original, fun );
    }

    /**
     * Takes two iterators as input and outputs one, using the merge function to 'merge' the head elements of each
     * input iterator into one element in the output iterator.
     * Returned iterator will have the same length as the shortest of the two input iterators
     *
     * @param in1
     * @param in2
     * @param mergeFun
     * @param <IN_1>
     * @param <IN_2>
     * @param <OUT>
     * @return
     */
    public <IN_1, IN_2, OUT> Iterator<OUT> merge( Iterator<IN_1> in1, Iterator<IN_2> in2,
            Function2<IN_1,IN_2,OUT,RuntimeException> mergeFun )
    {
        return new MergingGenerator<>( in1, in2, mergeFun );
    }

    /**
     * Assigns dependency times to all operations that do not yet have one assigned,
     * or to all if canOverwriteDependencyTime is true.
     * The dependency time assigned is equal to the scheduled start time of the previous operation,
     * starting with initialDependencyTime.
     * All operations in the returned iterator will have dependency times assigned to them.
     *
     * @param operations
     * @param initialDependencyTimeAsMilli
     * @param canOverwriteDependencyTime
     * @return
     */
    public Iterator<Operation> assignConservativeDependencyTimes( Iterator<Operation> operations,
            final long initialDependencyTimeAsMilli,
            final boolean canOverwriteDependencyTime )
    {
        Function1<Operation,Boolean,RuntimeException> isDependency = new Function1<Operation,Boolean,RuntimeException>()
        {
            @Override
            public Boolean apply( Operation operation )
            {
                return true;
            }
        };
        return assignDependencyTimesEqualToLastEncounteredDependencyTimeStamp( operations, isDependency,
                initialDependencyTimeAsMilli, canOverwriteDependencyTime );
    }

    /**
     * Assigns dependency times to all operations that do not yet have one assigned,
     * or to all if canOverwriteDependencyTime is true.
     * The dependency time assigned is equal to the scheduled start time of the last operation for which the
     * isDependency predicate returned true, starting with initialDependencyTime, as long as that time is lower than
     * current operations start time.
     * All operations in the returned iterator will have dependency times assigned to them.
     *
     * @param operations
     * @param isDependency
     * @param initialDependencyTimeAsMilli
     * @param canOverwriteDependencyTime
     * @return
     */
    public Iterator<Operation> assignDependencyTimesEqualToLastEncounteredLowerDependencyTimeStamp(
            Iterator<Operation> operations,
            final Function1<Operation,Boolean,RuntimeException> isDependency,
            final long initialDependencyTimeAsMilli,
            final boolean canOverwriteDependencyTime )
    {
        Function1<Operation,Operation,RuntimeException> dependencyTimeAssigningFun =
                new Function1<Operation,Operation,RuntimeException>()
                {
                    private long secondMostRecentDependencyAsMilli = initialDependencyTimeAsMilli;
                    private long mostRecentDependencyAsMilli = initialDependencyTimeAsMilli;

                    @Override
                    public Operation apply( Operation operation )
                    {
                        if ( -1 == operation.dependencyTimeStamp() || canOverwriteDependencyTime )
                        {
                            if ( operation.timeStamp() > mostRecentDependencyAsMilli )
                            { operation.setDependencyTimeStamp( mostRecentDependencyAsMilli ); }
                            else
                            { operation.setDependencyTimeStamp( secondMostRecentDependencyAsMilli ); }
                        }
                        if ( isDependency.apply( operation ) )
                        {
                            if ( operation.timeStamp() > mostRecentDependencyAsMilli )
                            {
                                secondMostRecentDependencyAsMilli = mostRecentDependencyAsMilli;
                                mostRecentDependencyAsMilli = operation.timeStamp();
                            }
                        }
                        return operation;
                    }
                };
        return new MappingGenerator<>( operations, dependencyTimeAssigningFun );
    }

    /**
     * Assigns dependency times to all operations that do not yet have one assigned,
     * or to all if canOverwriteDependencyTime is true.
     * The dependency time assigned is equal to the scheduled start time of the last operation for which the
     * isDependency predicate returned true, starting with initialDependencyTime.
     * All operations in the returned iterator will have dependency times assigned to them.
     *
     * @param operations
     * @param isDependency
     * @param initialDependencyTimeAsMilli
     * @param canOverwriteDependencyTime
     * @return
     */
    public Iterator<Operation> assignDependencyTimesEqualToLastEncounteredDependencyTimeStamp(
            Iterator<Operation> operations,
            final Function1<Operation,Boolean,RuntimeException> isDependency,
            final long initialDependencyTimeAsMilli,
            final boolean canOverwriteDependencyTime )
    {
        Function1<Operation,Operation,RuntimeException> dependencyTimeAssigningFun =
                new Function1<Operation,Operation,RuntimeException>()
                {
                    private long mostRecentDependencyAsMilli = initialDependencyTimeAsMilli;

                    @Override
                    public Operation apply( Operation operation )
                    {
                        if ( -1 == operation.dependencyTimeStamp() || canOverwriteDependencyTime )
                        { operation.setDependencyTimeStamp( mostRecentDependencyAsMilli ); }
                        if ( isDependency.apply( operation ) )
                        {
                            mostRecentDependencyAsMilli = operation.timeStamp();
                        }
                        return operation;
                    }
                };
        return new MappingGenerator<>( operations, dependencyTimeAssigningFun );
    }

    /**
     * Returns the same operation generator, with start times assigned to each operation taken from the start time
     * generator. Generator stops as soon as either of the generators, start times or operations, stops.
     *
     * @param startTimesAsMilli
     * @param operations
     * @return
     */
    public Iterator<Operation> assignStartTimes( Iterator<Long> startTimesAsMilli, Iterator<Operation> operations )
    {
        Function2<Long,Operation,Operation,RuntimeException> startTimeAssigningFun =
                new Function2<Long,Operation,Operation,RuntimeException>()
                {
                    @Override
                    public Operation apply( Long timeAsMilli, Operation operation )
                    {
                        operation.setScheduledStartTimeAsMilli( timeAsMilli );
                        operation.setTimeStamp( timeAsMilli );
                        return operation;
                    }
                };
        return new MergingGenerator<>( startTimesAsMilli, operations, startTimeAssigningFun );
    }

    /**
     * Returns the same operation generator, with dependency times assigned to each operation taken from the dependency
     * time
     * generator. Generator stops as soon as either of the generators, dependency times or operations, stops.
     *
     * @param dependencyTimesAsMilli
     * @param operations
     * @return
     */
    public Iterator<Operation> assignDependencyTimes( Iterator<Long> dependencyTimesAsMilli,
            Iterator<Operation> operations )
    {
        Function2<Long,Operation,Operation,RuntimeException> dependencyTimeAssigningFun =
                new Function2<Long,Operation,Operation,RuntimeException>()
                {
                    @Override
                    public Operation apply( Long timeAsMilli, Operation operation )
                    {
                        operation.setDependencyTimeStamp( timeAsMilli );
                        return operation;
                    }
                };
        return new MergingGenerator<>( dependencyTimesAsMilli, operations, dependencyTimeAssigningFun );
    }

    /**
     * Returns numbers, starting at specified number, and thereafter incrementing by a uniformly random amount
     * between the minimum and maximum amounts specified
     *
     * @param start
     * @param minIncrement
     * @param maxIncrement
     * @return
     */
    public <T extends Number> Iterator<T> randomIncrement( T start, T minIncrement, T maxIncrement )
    {
        Iterator<T> incrementAmountGenerator = uniform( minIncrement, maxIncrement );
        return incrementing( start, incrementAmountGenerator );
    }

    /**
     * Returned generator will merge all input generators into one, sorting on the scheduled start time of operations,
     * ascending
     *
     * @param generators
     * @return
     */
    public Iterator<Operation> mergeSortOperationsByScheduledStartTime( Iterator<Operation>... generators )
    {
        return mergeSort(
                new Comparator<Operation>()
                {
                    @Override
                    public int compare( Operation o1, Operation o2 )
                    {
                        if ( o1.scheduledStartTimeAsMilli() > o2.scheduledStartTimeAsMilli() )
                        { return 1; }
                        else if ( o1.scheduledStartTimeAsMilli() < o2.scheduledStartTimeAsMilli() )
                        { return -1; }
                        else
                        { return 0; }
                    }
                },
                generators
        );
    }

    /**
     * Returned generator will merge all input generators into one, sorting on the time stamp of operations, ascending
     *
     * @param generators
     * @return
     */
    public Iterator<Operation> mergeSortOperationsByTimeStamp( Iterator<Operation>... generators )
    {
        return mergeSort(
                new Comparator<Operation>()
                {
                    @Override
                    public int compare( Operation o1, Operation o2 )
                    {
                        if ( o1.timeStamp() > o2.timeStamp() )
                        { return 1; }
                        else if ( o1.timeStamp() < o2.timeStamp() )
                        { return -1; }
                        else
                        { return 0; }
                    }
                },
                generators
        );
    }

    /**
     * Returned generator will merge all input generators into one, sorting by value, ascending
     *
     * @param generators
     * @return
     */
    public <T extends Number> Iterator<T> mergeSortNumbers( Iterator<T>... generators )
    {
        return mergeSort(
                new Comparator<T>()
                {
                    @Override
                    public int compare( T t1, T t2 )
                    {
                        if ( t1.longValue() > t2.longValue() )
                        { return 1; }
                        else if ( t1.longValue() < t2.longValue() )
                        { return -1; }
                        else
                        { return 0; }
                    }
                },
                generators
        );
    }

    /**
     * Returned generator will merge all input generators into one, using provided comparator for sorting
     *
     * @param comparator
     * @param generators
     * @param <T>
     * @return
     */
    public <T> Iterator<T> mergeSort( Comparator<T> comparator, Iterator<T>... generators )
    {
        return Iterators.mergeSorted( Lists.newArrayList( generators ), comparator );
    }

    /**
     * Returned generator will merge all input generators into one, using provided comparator for sorting,
     * and looking ahead a bounded distance in case nearby elements of any one input generator are out of order
     *
     * @param comparator
     * @param lookAheadDistance
     * @param generators
     * @param <T>
     * @return
     */
    public <T> Iterator<T> mergeSort( Comparator<T> comparator, int lookAheadDistance, Iterator<T>... generators )
    {
        return new OrderedMultiGenerator<>( comparator, lookAheadDistance, generators );
    }

    /**
     * Returned generator will loop over input iterator indefinitely.
     * <p/>
     * CAUTION: the returned generator does NOT (can not) do a deep copy on the elements of the original generator.
     * As such, if elements of the original generator are not primitives the repeating generator will simply return
     * many references to the elements of the original generator, i.e., modifying any of them will modify the content
     * of all returned elements that are referenced by that element(/reference).
     *
     * @param generator
     * @param <T>
     * @return
     */
    public <T> Iterator<T> repeating( Iterator<T> generator )
    {
        return new RepeatingGenerator<>( generator );
    }

    /**
     * Returned generator will iterate over all of the things, once
     *
     * @param things
     * @param <T>
     * @return
     */
    public <T> Iterator<T> identity( T... things )
    {
        return new IdentityGenerator<>( things );
    }

    /**
     * Interleave every element from base generator with specific amount of elements from interleave with generator.
     * Returned generator will run until either base generator or interleave generator are exhausted.
     *
     * @param baseGenerator
     * @param interleaveWithGenerator
     * @param amountToInterleave
     * @param <T>
     * @return
     */
    public <T> Iterator<T> interleave( Iterator<? extends T> baseGenerator,
            Iterator<? extends T> interleaveWithGenerator, final int amountToInterleave )
    {
        Function0<Integer,RuntimeException> amountToInterleaveFun =
                new Function0<Integer,RuntimeException>()
                {
                    @Override
                    public Integer apply()
                    {
                        return amountToInterleave;
                    }
                };
        return new InterleaveGenerator<>( baseGenerator, interleaveWithGenerator, amountToInterleaveFun );
    }

    /**
     * Interleave every element from base generator with specific amount of elements from interleave with generator.
     * Returned generator will run until either base generator or interleave generator are exhausted.
     *
     * @param base
     * @param interleaveWith
     * @param amountToInterleaveFun
     * @param <T>
     * @return
     */
    public <T> Iterator<T> interleave( Iterator<? extends T> base, Iterator<? extends T> interleaveWith,
            Function0<Integer,RuntimeException> amountToInterleaveFun )
    {
        return new InterleaveGenerator<>( base, interleaveWith, amountToInterleaveFun );
    }

    /**
     * Offset start times of operations in stream such that first operation is now scheduled at new start time.
     *
     * @param generator
     * @param newStartTimeAsMilli
     * @return
     */
    public Iterator<Operation> timeOffset( Iterator<Operation> generator, long newStartTimeAsMilli )
    {
        return timeOffsetAndCompress( generator, newStartTimeAsMilli, null );
    }

    // TODO timeCompress (without offset)

    /**
     * Offset start times of operations in stream such that first operation is now scheduled at new start time.
     * Compress/expand duration between start times by a fixed ratio.
     * E.g. 2.0 = 2x slower, 0.5 = 2x faster
     *
     * @param generator
     * @param newStartTimeAsMilli
     * @param compressionRatio
     * @return
     */
    public Iterator<Operation> timeOffsetAndCompress( Iterator<Operation> generator, long newStartTimeAsMilli,
            Double compressionRatio )
    {
        return new TimeMappingOperationGenerator( generator, newStartTimeAsMilli, compressionRatio );
    }

    /**
     * Prefix every generated item with prefix string
     *
     * @param generator
     * @param prefix
     * @param <T>
     * @return
     */
    public <T> Iterator<String> prefix( Iterator<T> generator, final String prefix )
    {
        Function1<T,String,RuntimeException> prefixingFun =
                new Function1<T,String,RuntimeException>()
                {
                    @Override
                    public String apply( T item )
                    {
                        return prefix + item.toString();
                    }
                };
        return new MappingGenerator<>( generator, prefixingFun );
    }

    /**
     * Caps the amount of things generator can return
     *
     * @param generator
     * @param limit
     * @param <T>
     * @return
     */
    public <T> Iterator<T> limit( Iterator<T> generator, long limit )
    {
        return new LimitGenerator<T>( generator, limit );
    }

    /**
     * next() returns single item from set of items. Each item has equal probability of being chosen.
     *
     * @param items
     * @param <T>
     * @return
     */
    public <T> Iterator<T> discrete( Iterable<T> items )
    {
        List<Tuple2<Double,Iterator<T>>> weightedIteratorItems = new ArrayList<>();
        for ( T item : items )
        {
            weightedIteratorItems.add( Tuple.tuple2( 1d, constant( item ) ) );
        }
        return weightedDiscreteDereferencing( weightedIteratorItems );
    }

    /**
     * next() retrieves single iterator from set of iterators, then returns the next() element from that iterator.
     * All iterators are selected with equal probability
     *
     * @param itemIterators
     * @param <T>
     * @return
     */
    public <T> Iterator<T> discreteDereferencing( Iterable<Iterator<T>> itemIterators )
    {
        List<Tuple2<Double,Iterator<T>>> weightedIteratorItems = new ArrayList<>();
        for ( Iterator<T> itemIterator : itemIterators )
        {
            weightedIteratorItems.add( Tuple.tuple2( 1d, itemIterator ) );
        }
        return weightedDiscreteDereferencing( weightedIteratorItems );
    }

    /**
     * next() returns single item from set of items. Probability of selecting an item depends on weight assigned to
     * that
     * element.
     *
     * @param weightedItems
     * @param <T>
     * @return
     */
    public <T> Iterator<T> weightedDiscrete( Iterable<Tuple2<Double,T>> weightedItems )
    {
        List<Tuple2<Double,Iterator<T>>> weightedIteratorItems = new ArrayList<>();
        for ( Tuple2<Double,T> item : weightedItems )
        {
            weightedIteratorItems.add( Tuple.tuple2( item._1(), constant( item._2() ) ) );
        }
        return weightedDiscreteDereferencing( weightedIteratorItems );
    }

    /**
     * next() retrieves single iterator from set of iterators, then returns the next() element from that iterator.
     * Probability of selecting an iterator depends on weight assigned to that iterator.
     *
     * @param weightedIteratorItems
     * @param <T>
     * @return
     */
    public <T> Iterator<T> weightedDiscreteDereferencing( Iterable<Tuple2<Double,Iterator<T>>> weightedIteratorItems )
    {
        Iterator<Iterator<T>> discreteIteratorGenerator = new DiscreteGenerator<>( getRandom(), weightedIteratorItems );
        return new IteratorDereferencingGenerator<T>( discreteIteratorGenerator );
    }

    // TODO discreteList

    /**
     * next() returns list of multiple items from a collection of items.
     * Probability of selecting an item depends on weight assigned to that item.
     * amountToRetrieve specifies the size of the returned list
     *
     * @param items
     * @param amountToRetrieve
     * @param <T>
     * @return
     */
    public <T> Iterator<List<T>> weightedDiscreteList( Iterable<Tuple2<Double,T>> items, Integer amountToRetrieve )
    {
        Iterator<Integer> amountToRetrieveGenerator = constant( amountToRetrieve );
        return weightedDiscreteList( items, amountToRetrieveGenerator );
    }

    /**
     * next() returns list of multiple items from a collection of items.
     * Probability of selecting an item depends on weight assigned to that item.
     * amountToRetrieveGenerator.next() specifies the size of the returned list, so may vary between calls
     *
     * @param pairs
     * @param amountToRetrieveGenerator
     * @param <T>
     * @return
     */
    public <T> Iterator<List<T>> weightedDiscreteList( Iterable<Tuple2<Double,T>> pairs,
            Iterator<Integer> amountToRetrieveGenerator )
    {
        return new DiscreteListGenerator<T>( getRandom(), pairs, amountToRetrieveGenerator );
    }

    // TODO weightedDiscreteListDereferencing

    /**
     * next() returns a map.
     * Number of keys is specified by amountToRetrieve.
     * Values are generated by calling next() on the corresponding item's tuple.
     * The probability of a key (and its generated value) being returned depends on the weight assigned to its item
     * tuple.
     *
     * @param items
     * @param amountToRetrieve
     * @param <K>
     * @param <V>
     * @return
     */
    public <K, V> Iterator<Map<K,V>> weightedDiscreteMap( Iterable<Tuple3<Double,K,Iterator<V>>> items,
            Integer amountToRetrieve )
    {
        Iterator<Integer> amountToRetrieveGenerator = constant( amountToRetrieve );
        return weightedDiscreteMap( items, amountToRetrieveGenerator );
    }

    /**
     * next() returns a map.
     * Number of keys is specified by amountToRetrieve.next()
     * Values are generated by calling next() on the corresponding item's tuple.
     * The probability of a key (and its generated value) being returned depends on the weight assigned to its item
     * tuple.
     *
     * @param items
     * @param amountToRetrieveGenerator
     * @param <K>
     * @param <V>
     * @return
     */
    public <K, V> Iterator<Map<K,V>> weightedDiscreteMap( Iterable<Tuple3<Double,K,Iterator<V>>> items,
            Iterator<Integer> amountToRetrieveGenerator )
    {
        List<Tuple2<Double,Tuple2<K,Iterator<V>>>> probabilityItems =
                new ArrayList<Tuple2<Double,Tuple2<K,Iterator<V>>>>();
        for ( Tuple3<Double,K,Iterator<V>> item : items )
        {
            double thingProbability = item._1();
            Tuple2<K,Iterator<V>> thingGeneratorPair = Tuple.tuple2( item._2(), item._3() );
            probabilityItems.add( Tuple.tuple2( thingProbability, thingGeneratorPair ) );
        }

        Iterator<List<Tuple2<K,Iterator<V>>>> discreteListGenerator = weightedDiscreteList( probabilityItems,
                amountToRetrieveGenerator );

        Function1<List<Tuple2<K,Iterator<V>>>,Map<K,V>,RuntimeException> pairsToMap =
                new Function1<List<Tuple2<K,Iterator<V>>>,Map<K,V>,RuntimeException>()
                {
                    @Override
                    public Map<K,V> apply( List<Tuple2<K,Iterator<V>>> pairs )
                    {
                        Map<K,V> keyedValues = new HashMap<>();
                        for ( Tuple2<K,Iterator<V>> pair : pairs )
                        {
                            keyedValues.put( pair._1(), pair._2().next() );
                        }
                        return keyedValues;
                    }
                };
        return new MappingGenerator<>( discreteListGenerator, pairsToMap );
    }

    public <T extends Number> Iterator<T> naiveBoundedNumberRange( T lowerBound, T upperBound,
            Iterator<T> unboundedGenerator )
    {
        MinMaxGenerator<T> lowerBoundGenerator = minMaxGenerator( constant( lowerBound ), lowerBound, lowerBound );
        MinMaxGenerator<T> upperBoundGenerator = minMaxGenerator( constant( upperBound ), upperBound, upperBound );
        return new NaiveBoundedRangeNumberGenerator<T>( unboundedGenerator, lowerBoundGenerator, upperBoundGenerator );
    }

    /**
     * Wraps a number generator and ensures it only returns numbers within a min-max range.
     * Range is defined by lowerBoundGenerator and upperBoundGenerator.
     * Generator is naive because it simply calls next() on inner generator until a suitable (in range) number is
     * returned.
     *
     * @param lowerBoundGenerator
     * @param upperBoundGenerator
     * @param unboundedGenerator
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> naiveBoundedNumberRange( MinMaxGenerator<T> lowerBoundGenerator,
            MinMaxGenerator<T> upperBoundGenerator, Iterator<T> unboundedGenerator )
    {
        return new NaiveBoundedRangeNumberGenerator<T>( unboundedGenerator, lowerBoundGenerator, upperBoundGenerator );
    }


    /**
     * next() returns a uniform random number within a min-max range.
     * Range is defined by lowerBound and upperBound.
     *
     * @param lowerBound
     * @param upperBound
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> uniform( T lowerBound, T upperBound )
    {
        MinMaxGenerator<T> lowerBoundGenerator = minMaxGenerator( constant( lowerBound ), lowerBound, lowerBound );
        MinMaxGenerator<T> upperBoundGenerator = minMaxGenerator( constant( upperBound ), upperBound, upperBound );
        return dynamicRangeUniform( lowerBoundGenerator, upperBoundGenerator );
    }

    /**
     * next() returns an iterator that in turn returns uniform random bytes
     *
     * @return
     */
    public Iterator<Iterator<Byte>> sizedUniformBytesGenerator( Iterator<Long> lengths )
    {
        return new SizedUniformByteGeneratorGenerator( lengths, this );
    }

    /**
     * next() returns a uniform random byte
     *
     * @return
     */
    public Iterator<Byte> uniformBytes()
    {
        return new UniformByteGenerator( getRandom() );
    }

    /**
     * next() returns a uniform random number within a min-max range.
     * Range is defined by boundingGenerator.
     *
     * @param boundingGenerator
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> dynamicRangeUniform( MinMaxGenerator<T> boundingGenerator )
    {
        return dynamicRangeUniform( boundingGenerator, boundingGenerator );
    }

    /**
     * next() returns a uniform random number within a min-max range.
     * Range is defined by lowerBoundGenerator and upperBoundGenerator.
     *
     * @param lowerBoundGenerator
     * @param upperBoundGenerator
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> dynamicRangeUniform( MinMaxGenerator<T> lowerBoundGenerator,
            MinMaxGenerator<T> upperBoundGenerator )
    {
        return new DynamicRangeUniformNumberGenerator<T>( getRandom(), lowerBoundGenerator, upperBoundGenerator );
    }

    /**
     * next() always returns the same value, constant
     * <p/>
     * CAUTION: the returned generator does NOT (can not) do a deep copy on the constant parameter.
     * As such, if constant is not a primitives the generator will simply return many references to that same constant,
     * i.e., modifying any of them will modify all returned elements that are referenced by that element(/reference).
     *
     * @param constant
     * @param <T>
     * @return
     */
    public <T> Iterator<T> constant( T constant )
    {
        return new ConstantGenerator<T>( constant );
    }

    /**
     * next() returns start the first time it is called.
     * Subsequent calls return the number value returned in previous call increment by incrementBy.
     *
     * @param start
     * @param incrementBy
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> incrementing( T start, T incrementBy )
    {
        return boundedIncrementing( start, new ConstantGenerator<T>( incrementBy ), null );
    }

    /**
     * next() returns start the first time it is called.
     * Subsequent calls return the number value returned in previous call increment by the result of calling next() on
     * incrementByGenerator.
     *
     * @param start
     * @param incrementByGenerator
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> incrementing( T start, Iterator<T> incrementByGenerator )
    {
        return boundedIncrementing( start, incrementByGenerator, null );
    }

    /**
     * next() returns start the first time it is called.
     * Subsequent calls return the number value returned in previous call increment by incrementBy.
     * When max is reached the generator will be exhausted (hasNext()==false)
     *
     * @param start
     * @param incrementBy
     * @param max
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> boundedIncrementing( T start, T incrementBy, T max )
    {
        return boundedIncrementing( start, new ConstantGenerator<T>( incrementBy ), max );
    }

    /**
     * next() returns start the first time it is called.
     * Subsequent calls return the number value returned in previous call increment by the result of calling next() on
     * incrementByGenerator.
     * When max is reached the generator will be exhausted (hasNext()==false)
     *
     * @param start
     * @param incrementByGenerator
     * @param max
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> boundedIncrementing( T start, Iterator<T> incrementByGenerator, T max )
    {
        return new IncrementingGenerator<T>( start, incrementByGenerator, max );
    }

    /**
     * next() returns an exponential random number with given mean.
     *
     * @param mean
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> exponential( T mean )
    {
        return new ExponentialNumberGenerator<T>( getRandom(), mean );
    }

    /**
     * next() returns an exponential random number with given mean, in given min-max range.
     * Range is defined by lowerBoundGenerator and upperBoundGenerator.
     *
     * @param lowerBoundGenerator
     * @param upperBoundGenerator
     * @param mean
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> boundedRangeExponential( MinMaxGenerator<T> lowerBoundGenerator,
            MinMaxGenerator<T> upperBoundGenerator, T mean )
    {
        Iterator<T> generator = new ExponentialNumberGenerator<T>( getRandom(), mean );
        return naiveBoundedNumberRange( lowerBoundGenerator, upperBoundGenerator, generator );
    }
}