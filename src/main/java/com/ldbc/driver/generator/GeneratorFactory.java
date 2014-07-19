package com.ldbc.driver.generator;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.ldbc.driver.Operation;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.*;
import com.ldbc.driver.util.Tuple.Tuple2;
import com.ldbc.driver.util.Tuple.Tuple3;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.*;

public class GeneratorFactory {

    private final RandomDataGeneratorFactory randomDataGeneratorFactory;

    public GeneratorFactory(RandomDataGeneratorFactory randomDataGeneratorFactory) {
        this.randomDataGeneratorFactory = randomDataGeneratorFactory;
    }

    // Every returned generator (that takes a RandomDataGenerator as input) will use a different RandomDataGenerator
    RandomDataGenerator getRandom() {
        return randomDataGeneratorFactory.newRandom();
    }

    /*
     * ----------------------------------------------------------------------------------------------------
     * ---------------------------------------- UTILS -----------------------------------------------------
     * ----------------------------------------------------------------------------------------------------
     */

    /**
     * Compare operation streams by stream lengths and equality of the operations they contain
     *
     * @param operationStream1
     * @param operationStream2
     * @param compareTimes
     * @return
     */
    // TODO move into a separate class, test that class separately, use that class here
    // TODO and return an ENUM instead of a boolean, to provide information about what went wrong in case of failure
    public boolean compareOperationStreams(Iterator<Operation<?>> operationStream1,
                                           Iterator<Operation<?>> operationStream2,
                                           boolean compareTimes) {
        if (operationStream1.hasNext() != operationStream2.hasNext()) return false;

        while (operationStream1.hasNext()) {
            if (false == operationStream2.hasNext()) return false;
            Operation<?> next1 = operationStream1.next();
            Operation<?> next2 = operationStream2.next();
            if (null == next1 && null == next2) continue;
            if (null == next1 && null != next2) return false;
            if (null == next2 && null != next2) return false;

            if (false == next1.equals(next2)) return false;
            if (compareTimes) {
                Time scheduledStartTime1 = next1.scheduledStartTime();
                Time scheduledStartTime2 = next2.scheduledStartTime();
                if (null == scheduledStartTime1 && null != scheduledStartTime2) return false;
                if (null != scheduledStartTime1 && null == scheduledStartTime2) return false;
                if (null != scheduledStartTime1 && null != scheduledStartTime2)
                    if (false == scheduledStartTime1.equals(scheduledStartTime2)) return false;
                Time dependencyTime1 = next1.dependencyTime();
                Time dependencyTime2 = next2.dependencyTime();
                if (null == dependencyTime1 && null != dependencyTime2) return false;
                if (null != dependencyTime1 && null == dependencyTime2) return false;
                if (null != dependencyTime1 && null != dependencyTime2)
                    if (false == dependencyTime1.equals(dependencyTime2)) return false;
            }
        }
        if (operationStream2.hasNext()) return false;
        return true;
    }

    public <T> void exhaust(Iterator<T> generator) {
        while (generator.hasNext()) generator.next();
    }

    /*
     * ----------------------------------------------------------------------------------------------------
     * ---------------------------------------- DECORATORS ------------------------------------------------
     * ----------------------------------------------------------------------------------------------------
     */

    public static <T1> Iterator<T1> includeOnly(Iterator<T1> generator, T1... includedItems) {
        return Iterators.filter(generator, new IncludeOnlyPredicate<>(includedItems));
    }

    private static class IncludeOnlyPredicate<T1> implements Predicate<T1> {
        private final Set<T1> includedItems;

        private IncludeOnlyPredicate(T1... includedItems) {
            this.includedItems = new HashSet<>(Arrays.asList(includedItems));
        }

        @Override
        public boolean apply(T1 input) {
            return true == includedItems.contains(input);
        }
    }

    public static <T1> Iterator<T1> excludeAll(Iterator<T1> generator, T1... excludedItems) {
        return Iterators.filter(generator, new ExcludeAllPredicate<>(excludedItems));
    }

    private static class ExcludeAllPredicate<T1> implements Predicate<T1> {
        private final Set<T1> excludedItems;

        private ExcludeAllPredicate(T1... excludedItems) {
            this.excludedItems = new HashSet<>(Arrays.asList(excludedItems));
        }

        @Override
        public boolean apply(T1 input) {
            return false == excludedItems.contains(input);
        }
    }

    /**
     * Wraps any number generator and keeps track of the minimum and maximum numbers returned by that generator.
     *
     * @param generator
     * @param initialMin
     * @param initialMax
     * @param <T>
     * @return
     */
    public <T extends Number> MinMaxGenerator<T> minMaxGenerator(Iterator<T> generator, T initialMin, T initialMax) {
        return new MinMaxGenerator<T>(generator, initialMin, initialMax);
    }

    /*
     * ----------------------------------------------------------------------------------------------------
     * ---------------------------------------- GENERATORS ------------------------------------------------
     * ----------------------------------------------------------------------------------------------------
     */

    /**
     * Maps/transforms one iterator into another, using input function to perform the transformation on each individual element.
     * Returned iterator will have the same length input iterator.
     *
     * @param original
     * @param fun
     * @param <IN>
     * @param <OUT>
     * @return
     */
    public <IN, OUT> Iterator<OUT> map(Iterator<IN> original, Function1<IN, OUT> fun) {
        return new MappingGenerator<>(original, fun);
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
    public <IN_1, IN_2, OUT> Iterator<OUT> merge(Iterator<IN_1> in1, Iterator<IN_2> in2, Function2<IN_1, IN_2, OUT> mergeFun) {
        return new MergingGenerator<>(in1, in2, mergeFun);
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
     * @param initialDependencyTime
     * @param canOverwriteDependencyTime
     * @return
     */
    public Iterator<Operation<?>> assignConservativeDependencyTimes(Iterator<Operation<?>> operations,
                                                                    final Function1<Operation<?>, Boolean> isDependency,
                                                                    final Time initialDependencyTime,
                                                                    final boolean canOverwriteDependencyTime) {
        Function1<Operation<?>, Operation<?>> dependencyTimeAssigningFun = new Function1<Operation<?>, Operation<?>>() {
            private Time mostRecentDependency = initialDependencyTime;

            @Override
            public Operation<?> apply(Operation<?> operation) {
                if (null == operation.dependencyTime() || canOverwriteDependencyTime)
                    operation.setDependencyTime(mostRecentDependency);
                if (isDependency.apply(operation))
                    mostRecentDependency = operation.scheduledStartTime();
                return operation;
            }
        };
        return new MappingGenerator<>(operations, dependencyTimeAssigningFun);
    }

    /**
     * Returns the same operation generator, with start times assigned to each operation taken from the start time
     * generator. Generator stops as soon as either of the generators, start times or operations, stops.
     *
     * @param startTimes
     * @param operations
     * @return
     */
    public Iterator<Operation<?>> assignStartTimes(Iterator<Time> startTimes, Iterator<Operation<?>> operations) {
        Function2<Time, Operation<?>, Operation<?>> startTimeAssigningFun = new Function2<Time, Operation<?>, Operation<?>>() {
            @Override
            public Operation<?> apply(Time time, Operation<?> operation) {
                operation.setScheduledStartTime(time);
                return operation;
            }
        };
        return new MergingGenerator<>(startTimes, operations, startTimeAssigningFun);
    }

    /**
     * Returns the same operation generator, with dependency times assigned to each operation taken from the dependency time
     * generator. Generator stops as soon as either of the generators, dependency times or operations, stops.
     *
     * @param dependencyTimes
     * @param operations
     * @return
     */
    public Iterator<Operation<?>> assignDependencyTimes(Iterator<Time> dependencyTimes, Iterator<Operation<?>> operations) {
        Function2<Time, Operation<?>, Operation<?>> dependencyTimeAssigningFun = new Function2<Time, Operation<?>, Operation<?>>() {
            @Override
            public Operation<?> apply(Time time, Operation<?> operation) {
                operation.setDependencyTime(time);
                return operation;
            }
        };
        return new MergingGenerator<>(dependencyTimes, operations, dependencyTimeAssigningFun);
    }

    /**
     * Returns times, starting at specified start time, and thereafter incrementing by a uniformly random duration
     * between the minimum and maximum durations specified
     *
     * @param startTime
     * @param minIncrement
     * @param maxIncrement
     * @return
     */
    public Iterator<Time> randomIncrementTime(Time startTime, Duration minIncrement, Duration maxIncrement) {
        Iterator<Long> incrementTimeByGenerator = uniform(minIncrement.asMilli(), maxIncrement.asMilli());
        Iterator<Long> startTimeMilliSecondsGenerator = incrementing(startTime.asMilli(), incrementTimeByGenerator);
        return timeFromMilliSeconds(startTimeMilliSecondsGenerator);
    }

    /**
     * Returns times, starting at specified start time, and thereafter incrementing by exactly the specified duration increment
     *
     * @param startTime
     * @param increment
     * @return
     */
    public Iterator<Time> constantIncrementTime(Time startTime, Duration increment) {
        Iterator<Long> startTimeMilliSecondsGenerator = incrementing(startTime.asMilli(), increment.asMilli());
        return timeFromMilliSeconds(startTimeMilliSecondsGenerator);
    }

    private Iterator<Time> timeFromMilliSeconds(Iterator<Long> milliSecondsGenerator) {
        Function1<Long, Time> timeFromMilliFun = new Function1<Long, Time>() {
            @Override
            public Time apply(Long fromMilli) {
                return Time.fromMilli(fromMilli);
            }
        };
        return new MappingGenerator<>(milliSecondsGenerator, timeFromMilliFun);
    }

    /**
     * Returned generator will merge all input generators into one, sorting on the scheduled start time of operations, ascending
     *
     * @param generators
     * @return
     */
    public Iterator<Operation<?>> mergeSortOperationsByStartTime(Iterator<Operation<?>>... generators) {
        return mergeSort(new Comparator<Operation<?>>() {
            @Override
            public int compare(Operation<?> o1, Operation<?> o2) {
                return o1.scheduledStartTime().compareTo(o2.scheduledStartTime());
            }
        }, generators);
    }

    /**
     * Returned generator will merge all input generators into one, sorting by time, ascending
     *
     * @param generators
     * @return
     */
    public Iterator<Time> mergeSortTimes(Iterator<Time>... generators) {
        return mergeSort(new Comparator<Time>() {
            @Override
            public int compare(Time t1, Time t2) {
                return t1.compareTo(t2);
            }
        }, generators);
    }

    /**
     * Returned generator will merge all input generators into one, using provided comparator for sorting
     *
     * @param comparator
     * @param generators
     * @param <T>
     * @return
     */
    public <T> Iterator<T> mergeSort(Comparator<T> comparator, Iterator<T>... generators) {
        return mergeSort(comparator, 1, generators);
    }

    /**
     * Returned generator will merge all input generators into one, using provided comparator for sorting,
     * and looking ahead a bounded distance in case nearby elements of any one input generator are out of order
     *
     * @param comparator
     * @param lookaheadDistance
     * @param generators
     * @param <T>
     * @return
     */
    public <T> Iterator<T> mergeSort(Comparator<T> comparator, int lookaheadDistance, Iterator<T>... generators) {
        return new OrderedMultiGenerator<>(comparator, lookaheadDistance, generators);
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
    public <T> Iterator<T> repeating(Iterator<T> generator) {
        return new RepeatingGenerator<>(generator);
    }

    /**
     * Returned generator will iterate over all of the things, once
     *
     * @param things
     * @param <T>
     * @return
     */
    public <T> Iterator<T> identity(T... things) {
        return new IdentityGenerator<>(things);
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
    public <T> Iterator<T> interleave(Iterator<? extends T> baseGenerator, Iterator<? extends T> interleaveWithGenerator, final int amountToInterleave) {
        Function0<Integer> amountToInterleaveFun = new Function0<Integer>() {
            @Override
            public Integer apply() {
                return amountToInterleave;
            }
        };
        return new InterleaveGenerator<>(baseGenerator, interleaveWithGenerator, amountToInterleaveFun);
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
    public <T> Iterator<T> interleave(Iterator<? extends T> base, Iterator<? extends T> interleaveWith, Function0<Integer> amountToInterleaveFun) {
        return new InterleaveGenerator<>(base, interleaveWith, amountToInterleaveFun);
    }

    /**
     * Offset start times of operations in stream such that first operation is now scheduled at new start time.
     *
     * @param generator
     * @param newStartTime
     * @return
     */
    public Iterator<Operation<?>> timeOffset(Iterator<Operation<?>> generator, Time newStartTime) {
        return timeOffsetAndCompress(generator, newStartTime, null);
    }

    // TODO timeCompress (without offset)

    /**
     * Offset start times of operations in stream such that first operation is now scheduled at new start time.
     * Compress/expand duration between start times by a fixed ratio.
     * E.g. 2.0 = 2x slower, 0.5 = 2x faster
     *
     * @param generator
     * @param newStartTime
     * @param compressionRatio
     * @return
     */
    public Iterator<Operation<?>> timeOffsetAndCompress(Iterator<Operation<?>> generator, Time newStartTime, Double compressionRatio) {
        return new TimeMappingGenerator(generator, newStartTime, compressionRatio);
    }

    // TODO window generator

    /**
     * Prefix every generated item with prefix string
     *
     * @param generator
     * @param prefix
     * @param <T>
     * @return
     */
    public <T> Iterator<String> prefix(Iterator<T> generator, final String prefix) {
        Function1<T, String> prefixingFun = new Function1<T, String>() {
            @Override
            public String apply(T item) {
                return prefix + item.toString();
            }
        };
        return new MappingGenerator<>(generator, prefixingFun);
    }

    /**
     * Caps the amount of things generator can return
     *
     * @param generator
     * @param limit
     * @param <T>
     * @return
     */
    public <T> Iterator<T> limit(Iterator<T> generator, long limit) {
        return new LimitGenerator<T>(generator, limit);
    }

    /**
     * next() returns single item from set of items. Each item has equal probability of being chosen.
     *
     * @param items
     * @param <T>
     * @return
     */
    public <T> Iterator<T> discrete(Iterable<T> items) {
        List<Tuple2<Double, Iterator<T>>> weightedIteratorItems = new ArrayList<Tuple2<Double, Iterator<T>>>();
        for (T item : items) {
            weightedIteratorItems.add(Tuple.tuple2(1d, constant(item)));
        }
        return weightedDiscreteDereferencing(weightedIteratorItems);
    }

    /**
     * next() returns single item from set of items. Probability of selecting an item depends on weight assigned to that element.
     *
     * @param weightedItems
     * @param <T>
     * @return
     */
    public <T> Iterator<T> weightedDiscrete(Iterable<Tuple2<Double, T>> weightedItems) {
        List<Tuple2<Double, Iterator<T>>> weightedIteratorItems = new ArrayList<Tuple2<Double, Iterator<T>>>();
        for (Tuple2<Double, T> item : weightedItems) {
            weightedIteratorItems.add(Tuple.tuple2(item._1(), constant(item._2())));
        }
        return weightedDiscreteDereferencing(weightedIteratorItems);
    }

    /**
     * next() retrieves single iterator from set of iterators, then returns the next() element from that iterator.
     * Probability of selecting an iterator depends on weight assigned to that iterator.
     *
     * @param weightedIteratorItems
     * @param <T>
     * @return
     */
    public <T> Iterator<T> weightedDiscreteDereferencing(Iterable<Tuple2<Double, Iterator<T>>> weightedIteratorItems) {
        Iterator<Iterator<T>> discreteIteratorGenerator = new DiscreteGenerator<Iterator<T>>(getRandom(), weightedIteratorItems);
        return new IteratorDereferencingGenerator<T>(discreteIteratorGenerator);
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
    public <T> Iterator<List<T>> weightedDiscreteList(Iterable<Tuple2<Double, T>> items, Integer amountToRetrieve) {
        Iterator<Integer> amountToRetrieveGenerator = constant(amountToRetrieve);
        return weightedDiscreteList(items, amountToRetrieveGenerator);
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
    public <T> Iterator<List<T>> weightedDiscreteList(Iterable<Tuple2<Double, T>> pairs,
                                                      Iterator<Integer> amountToRetrieveGenerator) {
        return new DiscreteListGenerator<T>(getRandom(), pairs, amountToRetrieveGenerator);
    }

    // TODO weightedDiscreteListDereferencing

    /**
     * next() returns a map.
     * Number of keys is specified by amountToRetrieve.
     * Values are generated by calling next() on the corresponding item's tuple.
     * The probability of a key (and its generated value) being returned depends on the weight assigned to its item tuple.
     *
     * @param items
     * @param amountToRetrieve
     * @param <K>
     * @param <V>
     * @return
     */
    public <K, V> Iterator<Map<K, V>> weightedDiscreteMap(Iterable<Tuple3<Double, K, Iterator<V>>> items,
                                                          Integer amountToRetrieve) {
        Iterator<Integer> amountToRetrieveGenerator = constant(amountToRetrieve);
        return weightedDiscreteMap(items, amountToRetrieveGenerator);
    }

    /**
     * next() returns a map.
     * Number of keys is specified by amountToRetrieve.next()
     * Values are generated by calling next() on the corresponding item's tuple.
     * The probability of a key (and its generated value) being returned depends on the weight assigned to its item tuple.
     *
     * @param items
     * @param amountToRetrieveGenerator
     * @param <K>
     * @param <V>
     * @return
     */
    public <K, V> Iterator<Map<K, V>> weightedDiscreteMap(Iterable<Tuple3<Double, K, Iterator<V>>> items,
                                                          Iterator<Integer> amountToRetrieveGenerator) {
        List<Tuple2<Double, Tuple2<K, Iterator<V>>>> probabilityItems = new ArrayList<Tuple2<Double, Tuple2<K, Iterator<V>>>>();
        for (Tuple3<Double, K, Iterator<V>> item : items) {
            double thingProbability = item._1();
            Tuple2<K, Iterator<V>> thingGeneratorPair = Tuple.tuple2(item._2(), item._3());
            probabilityItems.add(Tuple.tuple2(thingProbability, thingGeneratorPair));
        }

        Iterator<List<Tuple2<K, Iterator<V>>>> discreteListGenerator = weightedDiscreteList(probabilityItems,
                amountToRetrieveGenerator);

        Function1<List<Tuple2<K, Iterator<V>>>, Map<K, V>> pairsToMap = new Function1<List<Tuple2<K, Iterator<V>>>, Map<K, V>>() {
            @Override
            public Map<K, V> apply(List<Tuple2<K, Iterator<V>>> pairs) {
                Map<K, V> keyedValues = new HashMap<>();
                for (Tuple2<K, Iterator<V>> pair : pairs) {
                    keyedValues.put(pair._1(), pair._2().next());
                }
                return keyedValues;
            }
        };
        return new MappingGenerator<>(discreteListGenerator, pairsToMap);
    }

    /**
     * RandomByteIteratorGenerator
     */
    public Iterator<ByteIterator> randomByteIterator(Integer length) {
        Iterator<Integer> lengthGenerator = constant(length);
        return randomByteIterator(lengthGenerator);
    }

    public Iterator<ByteIterator> randomByteIterator(Iterator<Integer> lengthGenerator) {
        return new RandomByteIteratorGenerator(getRandom(), lengthGenerator);
    }

    /**
     * Wraps a number generator and ensures it only returns numbers within a min-max range.
     * Range is defined by lowerBoundGenerator and upperBoundGenerator.
     * Generator is naive because it simply calls next() on inner generator until a suitable (in range) number is returned.
     *
     * @param lowerBoundGenerator
     * @param upperBoundGenerator
     * @param unboundedGenerator
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> naiveBoundedNumberRange(MinMaxGenerator<T> lowerBoundGenerator,
                                                                  MinMaxGenerator<T> upperBoundGenerator, Iterator<T> unboundedGenerator) {
        return new NaiveBoundedRangeNumberGenerator<T>(unboundedGenerator, lowerBoundGenerator, upperBoundGenerator);
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
    public <T extends Number> Iterator<T> uniform(T lowerBound, T upperBound) {
        MinMaxGenerator<T> lowerBoundGenerator = minMaxGenerator(constant(lowerBound), lowerBound, lowerBound);
        MinMaxGenerator<T> upperBoundGenerator = minMaxGenerator(constant(upperBound), upperBound, upperBound);
        return dynamicRangeUniform(lowerBoundGenerator, upperBoundGenerator);
    }

    /**
     * next() returns a uniform random number within a min-max range.
     * Range is defined by boundingGenerator.
     *
     * @param boundingGenerator
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> dynamicRangeUniform(MinMaxGenerator<T> boundingGenerator) {
        return dynamicRangeUniform(boundingGenerator, boundingGenerator);
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
    public <T extends Number> Iterator<T> dynamicRangeUniform(MinMaxGenerator<T> lowerBoundGenerator,
                                                              MinMaxGenerator<T> upperBoundGenerator) {
        return new DynamicRangeUniformNumberGenerator<T>(getRandom(), lowerBoundGenerator, upperBoundGenerator);
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
    public <T> Iterator<T> constant(T constant) {
        return new ConstantGenerator<T>(constant);
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
    public <T extends Number> Iterator<T> incrementing(T start, T incrementBy) {
        return boundedIncrementing(start, new ConstantGenerator<T>(incrementBy), null);
    }

    /**
     * next() returns start the first time it is called.
     * Subsequent calls return the number value returned in previous call increment by the result of calling next() on incrementByGenerator.
     *
     * @param start
     * @param incrementByGenerator
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> incrementing(T start, Iterator<T> incrementByGenerator) {
        return boundedIncrementing(start, incrementByGenerator, null);
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
    public <T extends Number> Iterator<T> boundedIncrementing(T start, T incrementBy, T max) {
        return boundedIncrementing(start, new ConstantGenerator<T>(incrementBy), max);
    }

    /**
     * next() returns start the first time it is called.
     * Subsequent calls return the number value returned in previous call increment by the result of calling next() on incrementByGenerator.
     * When max is reached the generator will be exhausted (hasNext()==false)
     *
     * @param start
     * @param incrementByGenerator
     * @param max
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> boundedIncrementing(T start, Iterator<T> incrementByGenerator, T max) {
        return new IncrementingGenerator<T>(start, incrementByGenerator, max);
    }

    /**
     * next() returns an exponential random number with given mean.
     *
     * @param mean
     * @param <T>
     * @return
     */
    public <T extends Number> Iterator<T> exponential(T mean) {
        return new ExponentialNumberGenerator<T>(getRandom(), mean);
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
    public <T extends Number> Iterator<T> boundedRangeExponential(MinMaxGenerator<T> lowerBoundGenerator,
                                                                  MinMaxGenerator<T> upperBoundGenerator, T mean) {
        Iterator<T> generator = new ExponentialNumberGenerator<T>(getRandom(), mean);
        return naiveBoundedNumberRange(lowerBoundGenerator, upperBoundGenerator, generator);
    }
}