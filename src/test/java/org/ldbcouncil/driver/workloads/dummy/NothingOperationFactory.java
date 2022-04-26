package org.ldbcouncil.driver.workloads.dummy;

import org.ldbcouncil.driver.Operation;

import java.util.Iterator;

public class NothingOperationFactory implements Iterator<Operation> {
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
