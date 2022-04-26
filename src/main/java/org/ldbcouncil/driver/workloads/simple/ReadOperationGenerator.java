package org.ldbcouncil.driver.workloads.simple;

import org.ldbcouncil.driver.Operation;
import org.ldbcouncil.driver.generator.Generator;
import org.ldbcouncil.driver.generator.GeneratorException;

import java.util.Iterator;
import java.util.List;

class ReadOperationGenerator extends Generator<Operation> {
    private final String table;
    private final Iterator<String> keyGenerator;
    private final Iterator<List<String>> fieldsGenerator;

    protected ReadOperationGenerator(String table, Iterator<String> keyGenerator, Iterator<List<String>> fieldsGenerator) {
        this.table = table;
        this.keyGenerator = keyGenerator;
        this.fieldsGenerator = fieldsGenerator;
    }

    @Override
    protected Operation doNext() throws GeneratorException {
        return new ReadOperation(table, keyGenerator.next(), fieldsGenerator.next());
    }
}
