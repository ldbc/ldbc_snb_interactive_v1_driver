package com.ldbc.driver.testutils;

import com.ldbc.driver.Operation;

public class NothingOperation extends Operation<DummyResult> {

    @Override
    public boolean equals(Object that) {
        return true;
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
