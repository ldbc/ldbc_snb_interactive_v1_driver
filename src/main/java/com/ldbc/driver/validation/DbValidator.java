package com.ldbc.driver.validation;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationHandlerRunnableContext;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;

import java.text.DecimalFormat;
import java.util.Iterator;

import static java.lang.String.format;

public class DbValidator
{
    public DbValidationResult validate( Iterator<ValidationParam> validationParameters,
            Db db,
            int validationParamsCount,
            Workload workload ) throws WorkloadException
    {
        System.out.println( "----" );
        DecimalFormat numberFormat = new DecimalFormat( "###,###,###,###,###" );
        DbValidationResult dbValidationResult = new DbValidationResult( db );
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ResultReporter resultReporter = new ResultReporter.SimpleResultReporter( errorReporter );

        int validationParamsProcessedSoFar = 0;
        int validationParamsCrashedSoFar = 0;
        int validationParamsIncorrectSoFar = 0;

        while ( validationParameters.hasNext() )
        {
            ValidationParam validationParam = validationParameters.next();
            Operation operation = validationParam.operation();
            Object expectedOperationResult = validationParam.operationResult();

            OperationHandlerRunnableContext handlerRunner;
            try
            {
                handlerRunner = db.getOperationHandlerRunnableContext( operation );
            }
            catch ( Throwable e )
            {

                dbValidationResult.reportMissingHandlerForOperation( operation );
                continue;
            }

            try
            {
                OperationHandler handler = handlerRunner.operationHandler();
                DbConnectionState dbConnectionState = handlerRunner.dbConnectionState();
                System.out.print( format(
                        "Processed %s / %s -- Crashed %s -- Incorrect %s -- Currently processing %s...\r",
                        numberFormat.format( validationParamsProcessedSoFar ),
                        numberFormat.format( validationParamsCount ),
                        numberFormat.format( validationParamsCrashedSoFar ),
                        numberFormat.format( validationParamsIncorrectSoFar ),
                        operation.getClass().getSimpleName()
                ) );
                handler.executeOperation( operation, dbConnectionState, resultReporter );
                if ( null == resultReporter.result() )
                {
                    throw new DbException(
                            format( "Db returned null result for: %s", operation.getClass().getSimpleName() ) );
                }
            }
            catch ( Throwable e )
            {
                // Not necessary, but perhaps useful for debugging
                e.printStackTrace();
                validationParamsCrashedSoFar++;
                dbValidationResult
                        .reportUnableToExecuteOperation( operation, ConcurrentErrorReporter.stackTraceToString( e ) );
                continue;
            }
            finally
            {
                validationParamsProcessedSoFar++;
                handlerRunner.cleanup();
            }

            Object actualOperationResult = resultReporter.result();

            if ( false == workload.resultsEqual( operation, expectedOperationResult, actualOperationResult ) )
            {
                validationParamsIncorrectSoFar++;
                dbValidationResult
                        .reportIncorrectResultForOperation( operation, expectedOperationResult, actualOperationResult );
                continue;
            }

            dbValidationResult.reportSuccessfulExecution( operation );
        }
        System.out.println( "----" );
        return dbValidationResult;
    }
}