package com.ldbc.driver.generator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class OrderedMultiGenerator<GENERATE_TYPE> extends Generator<GENERATE_TYPE> {
    private final List<GeneratorHead<GENERATE_TYPE>> generatorHeads;
    private final Comparator<GENERATE_TYPE> comparator;

    public OrderedMultiGenerator(Comparator<GENERATE_TYPE> comparator, int lookAheadDistance, Iterator<GENERATE_TYPE>... generators) {
        this.comparator = comparator;
        if (1 == lookAheadDistance) {
            this.generatorHeads = buildSimpleGeneratorHeads(generators);
        } else {
            this.generatorHeads = buildLookAheadGeneratorHeads(comparator, lookAheadDistance, generators);
        }
    }

    private static <T1> List<GeneratorHead<T1>> buildSimpleGeneratorHeads(Iterator<T1>... generators) {
        List<GeneratorHead<T1>> heads = new ArrayList<>();
        for (Iterator<T1> generator : generators) {
            heads.add(new SimpleGeneratorHead<>(generator));
        }
        return heads;
    }

    private static <T1> List<GeneratorHead<T1>> buildLookAheadGeneratorHeads(Comparator<T1> comparator, int distance, Iterator<T1>... generators) {
        List<GeneratorHead<T1>> heads = new ArrayList<>();
        for (Iterator<T1> generator : generators) {
            heads.add(new LookaheadGeneratorHead<>(generator, comparator, distance));
        }
        return heads;
    }

    @Override
    protected GENERATE_TYPE doNext() throws GeneratorException {
        GeneratorHead<GENERATE_TYPE> minGeneratorHead = getMinGeneratorHead();
        return (null == minGeneratorHead) ? null : minGeneratorHead.removeHead();
    }

    private GeneratorHead<GENERATE_TYPE> getMinGeneratorHead() {
        GeneratorHead<GENERATE_TYPE> minGeneratorHead = null;

        // Get generator head with lowest head element (removing empty ones encountered first)
        for (int i = 0; i < generatorHeads.size(); i++) {
            GeneratorHead<GENERATE_TYPE> generatorHead = generatorHeads.get(i);
            if (null == generatorHead) continue;
            if (null == generatorHead.inspectHead()) {
                generatorHeads.set(i, null);
                continue;
            }
            if (null == minGeneratorHead || comparator.compare(generatorHead.inspectHead(), minGeneratorHead.inspectHead()) < 0) {
                minGeneratorHead = generatorHead;
            }
        }

        return minGeneratorHead;
    }

    private static interface GeneratorHead<T1> {
        public T1 removeHead();

        public T1 inspectHead();
    }

    private static class SimpleGeneratorHead<T1> implements GeneratorHead<T1> {
        private final Iterator<T1> generator;
        private T1 head;

        public SimpleGeneratorHead(Iterator<T1> generator) {
            this.generator = generator;
            this.head = (this.generator.hasNext()) ? this.generator.next() : null;
        }

        @Override
        public T1 removeHead() {
            T1 oldHead = head;
            head = (generator.hasNext()) ? generator.next() : null;
            return oldHead;

        }

        @Override
        public T1 inspectHead() {
            return head;
        }
    }

    private static class LookaheadGeneratorHead<T1> implements GeneratorHead<T1> {
        private final Iterator<T1> generator;
        private final Comparator<T1> comparator;
        private final int lookaheadDistance;
        private List<T1> lookaheadBuffer;
        private T1 head;

        public LookaheadGeneratorHead(Iterator<T1> generator, Comparator<T1> comparator, int lookaheadDistance) {
            this.generator = generator;
            this.comparator = comparator;
            this.lookaheadDistance = lookaheadDistance;
            this.lookaheadBuffer = new ArrayList<>();
            fillLookaheadBuffer();
            this.head = getMinFromLookaheadBuffer();
        }

        @Override
        public T1 removeHead() {
            T1 oldHead = head;
            fillLookaheadBuffer();
            head = getMinFromLookaheadBuffer();
            return oldHead;

        }

        @Override
        public T1 inspectHead() {
            return head;
        }

        private T1 getMinFromLookaheadBuffer() {
            Iterator<T1> lookaheadBufferIterator = lookaheadBuffer.iterator();
            if (false == lookaheadBufferIterator.hasNext()) {
                return null;
            }
            int index = 0;
            int minIndex = index;
            T1 min = lookaheadBufferIterator.next();
            while (lookaheadBufferIterator.hasNext()) {
                index++;
                T1 next = lookaheadBufferIterator.next();
                if (comparator.compare(next, min) < 0) {
                    minIndex = index;
                    min = next;
                }
            }
            lookaheadBuffer.remove(minIndex);
            return min;
        }

        private void fillLookaheadBuffer() {
            while (generator.hasNext()) {
                if (lookaheadBuffer.size() >= lookaheadDistance) break;
                lookaheadBuffer.add(generator.next());
            }
        }
    }
}
