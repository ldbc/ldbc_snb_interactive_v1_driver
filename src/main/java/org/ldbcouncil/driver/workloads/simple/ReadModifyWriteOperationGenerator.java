package org.ldbcouncil.driver.workloads.simple;

import org.ldbcouncil.driver.Operation;
import org.ldbcouncil.driver.generator.Generator;
import org.ldbcouncil.driver.generator.GeneratorException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

class ReadModifyWriteOperationGenerator extends Generator<Operation> {
    private final String table;
    private final Iterator<String> keyGenerator;
    private final Iterator<List<String>> fieldsGenerator;
    private final Iterator<Map<String, Iterator<Byte>>> valuedFieldsGenerator;

    protected ReadModifyWriteOperationGenerator(String table, Iterator<String> keyGenerator,
                                                Iterator<List<String>> fieldsGenerator, Iterator<Map<String, Iterator<Byte>>> valuedFieldsGenerator) {
        this.table = table;
        this.keyGenerator = keyGenerator;
        this.fieldsGenerator = fieldsGenerator;
        this.valuedFieldsGenerator = valuedFieldsGenerator;
    }

    @Override
    protected Operation doNext() throws GeneratorException {
        return new ReadModifyWriteOperation(table, keyGenerator.next(), fieldsGenerator.next(), valuedFieldsGenerator.next());
    }
}
