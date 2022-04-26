package org.ldbcouncil.driver.workloads.simple;

import org.ldbcouncil.driver.Operation;
import org.ldbcouncil.driver.generator.Generator;
import org.ldbcouncil.driver.generator.GeneratorException;

import java.util.Iterator;
import java.util.Map;

class InsertOperationGenerator extends Generator<Operation> {
    private final String table;
    private final Iterator<String> keyGenerator;
    private final Iterator<Map<String, Iterator<Byte>>> valuedFieldsGenerator;

    protected InsertOperationGenerator(String table, Iterator<String> keyGenerator,
                                       Iterator<Map<String, Iterator<Byte>>> valuedFieldsGenerator) {
        this.table = table;
        this.keyGenerator = keyGenerator;
        this.valuedFieldsGenerator = valuedFieldsGenerator;
    }

    @Override
    protected Operation doNext() throws GeneratorException {
        return new InsertOperation(table, keyGenerator.next(), valuedFieldsGenerator.next());
    }
}