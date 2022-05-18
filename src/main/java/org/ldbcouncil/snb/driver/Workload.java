package org.ldbcouncil.snb.driver;

import org.ldbcouncil.snb.driver.control.DriverConfiguration;
import org.ldbcouncil.snb.driver.generator.GeneratorFactory;
import org.ldbcouncil.snb.driver.validation.ResultsLogValidationTolerances;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class Workload implements Closeable
{
    public static final long DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE_AS_MILLI = TimeUnit.HOURS.toMillis( 1 );

    private boolean isInitialized = false;
    private boolean isClosed = false;

    public abstract Map<Integer,Class<? extends Operation>> operationTypeToClassMapping();

    /**
     * Calculates the total tolerated delayed operations. Note that this is the total over all the operations
     * and not for each individual operation type.
     * @param configuration
     * @param warmup
     * @return
     */
    public ResultsLogValidationTolerances resultsLogValidationTolerances(
            DriverConfiguration configuration,
            boolean warmup
    )
    {
        long excessiveDelayThresholdAsMilli = TimeUnit.SECONDS.toMillis( 1 );
        double toleratedExcessiveDelayCountPercentage = 0.05; // 95% of the queries must run below delay threshold
        long toleratedExcessiveDelayCount = // Total tolerated excessive delay count
                (warmup) ? Math.round( configuration.warmupCount() * toleratedExcessiveDelayCountPercentage )
                         : Math.round( configuration.operationCount() * toleratedExcessiveDelayCountPercentage );
        return new ResultsLogValidationTolerances(
                excessiveDelayThresholdAsMilli,
                toleratedExcessiveDelayCount
        );
    }

    /**
     * Called once to initialize state for workload
     */
    public final void init( DriverConfiguration params ) throws WorkloadException
    {
        if ( isInitialized )
        { throw new WorkloadException( "Workload may be initialized only once" ); }
        isInitialized = true;
        onInit( params.asMap() );
    }

    public abstract void onInit( Map<String,String> params ) throws WorkloadException;


    public final void close() throws IOException
    {
        if ( isClosed )
        {
            throw new IOException( "Workload may be cleaned up only once" );
        }
        isClosed = true;
        onClose();
    }

    protected abstract void onClose() throws IOException;

    public final WorkloadStreams streams( GeneratorFactory gf, boolean hasDbConnected ) throws WorkloadException
    {
        if ( false == isInitialized )
        { throw new WorkloadException( "Workload has not been initialized" ); }
        return getStreams( gf, hasDbConnected );
    }

    protected abstract WorkloadStreams getStreams( GeneratorFactory generators, boolean hasDbConnected )
            throws WorkloadException;

    public DbValidationParametersFilter dbValidationParametersFilter( final Integer requiredValidationParameterCount )
    {
        return new DbValidationParametersFilter()
        {
            private final List<Operation> injectedOperations = new ArrayList<>();
            int validationParameterCount = 0;

            @Override
            public boolean useOperation( Operation operation )
            {
                return true;
            }

            @Override
            public DbValidationParametersFilterResult useOperationAndResultForValidation(
                    Operation operation,
                    Object operationResult )
            {
                if ( validationParameterCount < requiredValidationParameterCount )
                {
                    validationParameterCount++;
                    return new DbValidationParametersFilterResult(
                            DbValidationParametersFilterAcceptance.ACCEPT_AND_CONTINUE,
                            injectedOperations
                    );
                }
                else
                {
                    return new DbValidationParametersFilterResult(
                            DbValidationParametersFilterAcceptance.REJECT_AND_FINISH,
                            injectedOperations
                    );
                }
            }
        };
    }

    public long maxExpectedInterleaveAsMilli()
    {
        return DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE_AS_MILLI;
    }

    public abstract int enabledValidationOperations();

    public interface DbValidationParametersFilter
    {
        boolean useOperation( Operation operation );

        DbValidationParametersFilterResult useOperationAndResultForValidation(
                Operation operation,
                Object operationResult );
    }

    public enum DbValidationParametersFilterAcceptance
    {
        ACCEPT_AND_CONTINUE,
        ACCEPT_AND_FINISH,
        REJECT_AND_CONTINUE,
        REJECT_AND_FINISH;
    }

    public static class DbValidationParametersFilterResult
    {
        private final DbValidationParametersFilterAcceptance acceptance;
        private final List<Operation> injectedOperations;

        public DbValidationParametersFilterResult(
                DbValidationParametersFilterAcceptance acceptance,
                List<Operation> injectedOperations )
        {
            this.acceptance = acceptance;
            this.injectedOperations = injectedOperations;
        }

        public DbValidationParametersFilterAcceptance acceptance()
        {
            return acceptance;
        }

        public List<Operation> injectedOperations()
        {
            return injectedOperations;
        }
    }

}
