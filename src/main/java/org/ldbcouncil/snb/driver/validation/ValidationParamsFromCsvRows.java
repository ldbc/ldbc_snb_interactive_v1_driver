package org.ldbcouncil.snb.driver.validation;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.Workload;
import org.ldbcouncil.snb.driver.generator.GeneratorException;

import java.util.Iterator;

import com.fasterxml.jackson.databind.ObjectMapper;

import static java.lang.String.format;

import java.io.IOException;

public class ValidationParamsFromCsvRows implements Iterator<ValidationParam>
{
    private final Iterator<String[]> csvRows;
    private final Class<? extends Operation> operationClass;
    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public ValidationParamsFromCsvRows( Iterator<String[]> csvRows, Workload workload )
    {
        this.csvRows = csvRows;
        this.operationClass = workload.getOperationClass();
    }

    @Override
    public boolean hasNext()
    {
        return csvRows.hasNext();
    }

    @Override
    public ValidationParam next()
    {
        String[] csvRow = csvRows.next();
        String serializedOperation = csvRow[0];
        String serializedOperationResult = csvRow[1];

        Operation operation;
        try
        {
            operation = OBJECT_MAPPER.readValue(serializedOperation, this.operationClass);
        }
        catch ( IOException e )
        {
            throw new GeneratorException( format( "Error marshalling operation\n%s", serializedOperation ), e );
        }

        Object operationResult;
        try
        {
                operationResult = operation.deserializeResult( serializedOperationResult );
        }
        catch ( IOException e )
        {
            throw new GeneratorException( format( "Error marshalling operation result\n%s", serializedOperationResult ),
                    e );
        }
        return ValidationParam.createUntyped( operation, operationResult );
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( "remove() not supported by " + getClass().getName() );
    }
}
