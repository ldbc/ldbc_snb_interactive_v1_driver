package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;

import java.util.Iterator;
import java.util.List;

/**
 * Created by alexaverbuch on 7/11/14.
 */
class ScanOperationGenerator extends Generator<Operation> {
    private final String table;
    private final Iterator<String> startKeyGenerator;
    private final Iterator<Integer> recordCountGenerator;
    private final Iterator<List<String>> fieldsGenerator;

    protected ScanOperationGenerator(String table, Iterator<String> startKeyGenerator,
                                     Iterator<Integer> recordCountGenerator, Iterator<List<String>> fieldsGenerator) {
        this.table = table;
        this.startKeyGenerator = startKeyGenerator;
        this.recordCountGenerator = recordCountGenerator;
        this.fieldsGenerator = fieldsGenerator;
    }

    @Override
    protected Operation doNext() throws GeneratorException {
        return new ScanOperation(table, startKeyGenerator.next(), recordCountGenerator.next(), fieldsGenerator.next());
    }
}
