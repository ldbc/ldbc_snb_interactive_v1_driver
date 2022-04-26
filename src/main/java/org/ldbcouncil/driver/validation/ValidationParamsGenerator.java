package org.ldbcouncil.driver.validation;

import org.ldbcouncil.driver.Db;
import org.ldbcouncil.driver.DbConnectionState;
import org.ldbcouncil.driver.DbException;
import org.ldbcouncil.driver.Operation;
import org.ldbcouncil.driver.OperationHandler;
import org.ldbcouncil.driver.OperationHandlerRunnableContext;
import org.ldbcouncil.driver.ResultReporter;
import org.ldbcouncil.driver.Workload;
import org.ldbcouncil.driver.Workload.DbValidationParametersFilter;
import org.ldbcouncil.driver.Workload.DbValidationParametersFilterResult;
import org.ldbcouncil.driver.generator.Generator;
import org.ldbcouncil.driver.generator.GeneratorException;
import org.ldbcouncil.driver.runtime.ConcurrentErrorReporter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;

public class ValidationParamsGenerator extends Generator<ValidationParam>
{
    private final Db db;
    private final DbValidationParametersFilter dbValidationParametersFilter;
    private final Iterator<Operation> operations;
    private final ResultReporter resultReporter;
    private int entriesWrittenSoFar;
    private boolean needMoreValidationParameters;
    private final List<Operation> injectedOperations;

    public ValidationParamsGenerator( Db db,
            DbValidationParametersFilter dbValidationParametersFilter,
            Iterator<Operation> operations )
    {
        this.db = db;
        this.dbValidationParametersFilter = dbValidationParametersFilter;
        this.operations = operations;
        this.resultReporter = new ResultReporter.SimpleResultReporter( new ConcurrentErrorReporter() );
        this.entriesWrittenSoFar = 0;
        this.needMoreValidationParameters = true;
        this.injectedOperations = new ArrayList<>();
    }

    public int entriesWrittenSoFar()
    {
        return entriesWrittenSoFar;
    }

    @Override
    protected ValidationParam doNext() throws GeneratorException
    {
        while ( (injectedOperations.size() > 0 || operations.hasNext()) && needMoreValidationParameters )
        {
            Operation operation;
            if ( injectedOperations.isEmpty() )
            {
                operation = operations.next();
            }
            else
            {
                operation = injectedOperations.remove( 0 );
            }

            if ( false == dbValidationParametersFilter.useOperation( operation ) )
            { continue; }

            OperationHandlerRunnableContext operationHandlerRunner;
            try
            {
                operationHandlerRunner = db.getOperationHandlerRunnableContext( operation );
            }
            catch ( DbException e )
            {
                throw new GeneratorException(
                        format(
                                "Error retrieving operation handler for operation\n"
                                + "Db: %s\n"
                                + "Operation: %s",
                                db.getClass().getName(), operation ),
                        e );
            }
            try
            {
                OperationHandler operationHandler = operationHandlerRunner.operationHandler();
                DbConnectionState dbConnectionState = operationHandlerRunner.dbConnectionState();
                operationHandler.executeOperation( operation, dbConnectionState, resultReporter );
            }
            catch ( DbException e )
            {
                throw new GeneratorException(
                        format( ""
                                + "Error executing operation to retrieve validation result\n"
                                + "Db: %s\n"
                                + "Operation: %s",
                                db.getClass().getName(), operation ),
                        e );
            }
            finally
            {
                operationHandlerRunner.cleanup();
            }

            Object result = resultReporter.result();
            DbValidationParametersFilterResult dbValidationParametersFilterResult =
                    dbValidationParametersFilter.useOperationAndResultForValidation( operation, result );
            injectedOperations.addAll( dbValidationParametersFilterResult.injectedOperations() );

            switch ( dbValidationParametersFilterResult.acceptance() )
            {
            case REJECT_AND_CONTINUE:
                continue;
            case REJECT_AND_FINISH:
                needMoreValidationParameters = false;
                continue;
            case ACCEPT_AND_CONTINUE:
                entriesWrittenSoFar++;
                return ValidationParam.createUntyped( operation, result );
            case ACCEPT_AND_FINISH:
                entriesWrittenSoFar++;
                needMoreValidationParameters = false;
                return ValidationParam.createUntyped( operation, result );
            default:
                throw new GeneratorException(
                        format( "Unrecognized %s value: %s",
                                Workload.DbValidationParametersFilterAcceptance.class.getSimpleName(),
                                dbValidationParametersFilterResult.acceptance().name()
                        )
                );
            }
        }
        // ran out of operations OR validation set size has been reached
        return null;
    }
}