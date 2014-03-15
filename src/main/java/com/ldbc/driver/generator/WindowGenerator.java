package com.ldbc.driver.generator;

import com.google.common.base.Function;

import java.util.Iterator;

public class WindowGenerator<WINDOW_INPUT_TYPE, WINDOW_TYPE extends Window<WINDOW_INPUT_TYPE, ?>> extends Generator<WINDOW_TYPE> {
    public static enum PartialWindowStrategy {
        ERROR,
        DISCARD,
        RETURN
    }

    private static <W extends Window> Function<W, W> strategyEnumToStrategyClass(PartialWindowStrategy partialWindowStrategy) {
        switch (partialWindowStrategy) {
            case ERROR:
                return new ErrorStrategy<W>();
            case DISCARD:
                return new DiscardStrategy<W>();
            case RETURN:
                return new ReturnStrategy<W>();
        }
        throw new GeneratorException(String.format("Unrecognized strategy: ", partialWindowStrategy.name()));
    }

    private final Iterator<WINDOW_INPUT_TYPE> things;
    private final Iterator<WINDOW_TYPE> windows;
    private final Function<WINDOW_TYPE, WINDOW_TYPE> partialWindowStrategy;

    private WINDOW_INPUT_TYPE nextThing = null;

    public WindowGenerator(Iterator<WINDOW_INPUT_TYPE> things,
                           Iterator<WINDOW_TYPE> windows,
                           PartialWindowStrategy partialWindowStrategy) {
        this(things, windows, WindowGenerator.<WINDOW_TYPE>strategyEnumToStrategyClass(partialWindowStrategy));
    }

    private WindowGenerator(Iterator<WINDOW_INPUT_TYPE> things,
                            Iterator<WINDOW_TYPE> windows,
                            Function<WINDOW_TYPE, WINDOW_TYPE> partialWindowStrategy) {
        this.things = things;
        this.windows = windows;
        this.partialWindowStrategy = partialWindowStrategy;
    }

    @Override
    protected WINDOW_TYPE doNext() {
        if (false == windows.hasNext()) {
            // No more windows
            return null;
        }
        if (false == things.hasNext() && null == nextThing) {
            // No more things
            return null;
        }
        WINDOW_TYPE window = windows.next();

        if (null != nextThing) {
            if (window.add(nextThing)) {
                nextThing = null;
            } else {
                // Last read thing does not fit in this window, return empty window
                return window;
            }
        }

        while (things.hasNext()) {
            nextThing = things.next();
            if (window.add(nextThing)) {
                // Thing fits in window, continue
            } else {
                // Thing does not fit in window. Window full, return window contents
                return window;
            }
        }

        nextThing = null;

        // No more things, things finished before window finished: partial window
        if (window.isComplete()) {
            return window;
        } else {
            return partialWindowStrategy.apply(window);
        }
    }

    private static class ErrorStrategy<W extends Window> implements Function<W, W> {
        @Override
        public W apply(W partialWindow) {
            throw new GeneratorException(String.format("%s encountered a partial window: %s",
                    WindowGenerator.class.getSimpleName(), partialWindow.toString()));
        }
    }

    private static class DiscardStrategy<W extends Window> implements Function<W, W> {
        @Override
        public W apply(W partialWindow) {
            return null;
        }
    }

    private static class ReturnStrategy<W extends Window> implements Function<W, W> {
        @Override
        public W apply(W partialWindow) {
            return partialWindow;
        }
    }
}