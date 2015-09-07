package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.generator.GeneratorException;

import java.util.Iterator;

import static java.lang.String.format;

public class ValidationParamsFromCsvRows implements Iterator<ValidationParam>
{
    private final Iterator<String[]> csvRows;
    private final Workload workload;

    public ValidationParamsFromCsvRows( Iterator<String[]> csvRows, Workload workload )
    {
        this.csvRows = csvRows;
        this.workload = workload;
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
            operation = workload.marshalOperation( serializedOperation );
        }
        catch ( SerializingMarshallingException e )
        {
            throw new GeneratorException( format( "Error marshalling operation\n%s", serializedOperation ), e );
        }

        Object operationResult;
        try
        {
            operationResult = operation.marshalResult( serializedOperationResult );
        }
        catch ( SerializingMarshallingException e )
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
