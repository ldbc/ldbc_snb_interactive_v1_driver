package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.Operation;

public class NothingOperation extends Operation<DummyResult> {
    @Override
    public boolean equals(Object that) {
        return true;
    }

    @Override
    public int type() {
        return 0;
    }

    @Override
    public DummyResult marshalResult(String serializedOperationResult) {
        return new DummyResult();
    }

    @Override
    public String serializeResult(Object operationResultInstance) {
        return "";
    }
}
