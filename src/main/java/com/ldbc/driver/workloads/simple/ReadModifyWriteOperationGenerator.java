package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;
import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by alexaverbuch on 7/11/14.
 */
class ReadModifyWriteOperationGenerator extends Generator<Operation<?>> {
    private final String table;
    private final Iterator<String> keyGenerator;
    private final Iterator<List<String>> fieldsGenerator;
    private final Iterator<Map<String, ByteIterator>> valuedFieldsGenerator;

    protected ReadModifyWriteOperationGenerator(String table, Iterator<String> keyGenerator,
                                                Iterator<List<String>> fieldsGenerator, Iterator<Map<String, ByteIterator>> valuedFieldsGenerator) {
        this.table = table;
        this.keyGenerator = keyGenerator;
        this.fieldsGenerator = fieldsGenerator;
        this.valuedFieldsGenerator = valuedFieldsGenerator;
    }

    @Override
    protected Operation<?> doNext() throws GeneratorException {
        return new ReadModifyWriteOperation(table, keyGenerator.next(), fieldsGenerator.next(),
                valuedFieldsGenerator.next());
    }
}
