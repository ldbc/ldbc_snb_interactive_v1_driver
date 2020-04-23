package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;

import java.util.ArrayList;
import java.util.List;


public interface   ParamsFilter {


     boolean useOp(Operation operation);

     FilterResult useOpAndRes(Operation operation, Object operationResult);
}


