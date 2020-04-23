package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;

public class LdbcSnbBiParamsFilter implements ParamsFilter {

    @Override
    public boolean useOp(Operation operation) {
        return false;
    }

    @Override
    public FilterResult useOpAndRes(Operation operation, Object operationResult) {
        return null;
    }
}
