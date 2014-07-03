package com.ldbc.driver.testutils;

import java.util.Iterator;

public class NothingOperationFactory implements Iterator<NothingOperation> {
    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public NothingOperation next() {
        return new NothingOperation();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
