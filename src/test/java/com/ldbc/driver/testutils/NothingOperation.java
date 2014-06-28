package com.ldbc.driver.testutils;

import com.ldbc.driver.Operation;

public class NothingOperation extends Operation<Object> {
    @Override
    public Object marshalResult(String serializedOperationResult) {
        return null;
    }

    @Override
    public String serializeResult(Object operationResultInstance) {
        return null;
    }
}
