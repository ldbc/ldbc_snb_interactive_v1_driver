package com.ldbc.driver.workloads.ldbc.snb.bi.db;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9Result;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class DummyLdbcSnbBiDb extends Db
{
    private static class DummyDbConnectionState extends DbConnectionState
    {
        @Override
        public void close() throws IOException
        {
        }
    }

    public enum SleepType
    {
        THREAD_SLEEP,
        SPIN
    }

    public static final String SLEEP_DURATION_NANO_ARG = "ldbc.snb.bi.db.sleep_duration_nano";
    public static final String SLEEP_TYPE_ARG = "ldbc.snb.bi.db.sleep_type";

    private static long sleepDurationAsNano;
    private SleepType sleepType;

    private interface SleepFun
    {
        void sleep( long sleepNs );
    }

    private static SleepFun sleepFun;

    @Override
    protected void onInit( Map<String,String> properties, LoggingService loggingService ) throws DbException
    {
        String sleepDurationAsNanoAsString = properties.get( SLEEP_DURATION_NANO_ARG );
        if ( null == sleepDurationAsNanoAsString )
        {
            sleepDurationAsNano = 0l;
        }
        else
        {
            try
            {
                sleepDurationAsNano = Long.parseLong( sleepDurationAsNanoAsString );
            }
            catch ( NumberFormatException e )
            {
                throw new DbException( format( "Error encountered while trying to parse value [%s] for %s",
                        sleepDurationAsNanoAsString, SLEEP_DURATION_NANO_ARG ), e );
            }
        }
        String sleepTypeString = properties.get( SLEEP_TYPE_ARG );
        if ( null == sleepTypeString )
        {
            sleepType = SleepType.SPIN;
        }
        else
        {
            try
            {
                sleepType = SleepType.valueOf( properties.get( SLEEP_TYPE_ARG ) );
            }
            catch ( IllegalArgumentException e )
            {
                throw new DbException( format( "Invalid sleep type: %s", sleepTypeString ) );
            }
        }

        if ( 0 == sleepDurationAsNano )
        {
            sleepFun = new SleepFun()
            {
                @Override
                public void sleep( long sleepNs )
                {
                    // do nothing
                }
            };
        }
        else
        {
            switch ( sleepType )
            {
            case THREAD_SLEEP:
                sleepFun = new SleepFun()
                {
                    @Override
                    public void sleep( long sleepNs )
                    {
                        try
                        {
                            Thread.sleep( TimeUnit.NANOSECONDS.toMillis( sleepNs ) );
                        }
                        catch ( InterruptedException e )
                        {
                            // do nothing
                        }
                    }
                };
                break;
            case SPIN:
                sleepFun = new SleepFun()
                {
                    @Override
                    public void sleep( long sleepNs )
                    {
                        long endTimeAsNano = System.nanoTime() + sleepNs;
                        while ( System.nanoTime() < endTimeAsNano )
                        {
                            // busy wait
                        }
                    }
                };
                break;
            }
        }

        properties.put( SLEEP_DURATION_NANO_ARG, Long.toString( sleepDurationAsNano ) );
        properties.put( SLEEP_TYPE_ARG, sleepType.name() );

        // Long Reads
        registerOperationHandler( LdbcSnbBiQuery1.class, LdbcQuery1Handler.class );
        registerOperationHandler( LdbcSnbBiQuery2.class, LdbcQuery2Handler.class );
        registerOperationHandler( LdbcSnbBiQuery3.class, LdbcQuery3Handler.class );
        registerOperationHandler( LdbcSnbBiQuery4.class, LdbcQuery4Handler.class );
        registerOperationHandler( LdbcSnbBiQuery5.class, LdbcQuery5Handler.class );
        registerOperationHandler( LdbcSnbBiQuery6.class, LdbcQuery6Handler.class );
        registerOperationHandler( LdbcSnbBiQuery7.class, LdbcQuery7Handler.class );
        registerOperationHandler( LdbcSnbBiQuery8.class, LdbcQuery8Handler.class );
        registerOperationHandler( LdbcSnbBiQuery9.class, LdbcQuery9Handler.class );
        registerOperationHandler( LdbcSnbBiQuery10.class, LdbcQuery10Handler.class );
        registerOperationHandler( LdbcSnbBiQuery11.class, LdbcQuery11Handler.class );
        registerOperationHandler( LdbcSnbBiQuery12.class, LdbcQuery12Handler.class );
        registerOperationHandler( LdbcSnbBiQuery13.class, LdbcQuery13Handler.class );
        registerOperationHandler( LdbcSnbBiQuery14.class, LdbcQuery14Handler.class );
        registerOperationHandler( LdbcSnbBiQuery15.class, LdbcQuery15Handler.class );
        registerOperationHandler( LdbcSnbBiQuery16.class, LdbcQuery16Handler.class );
        registerOperationHandler( LdbcSnbBiQuery17.class, LdbcQuery17Handler.class );
        registerOperationHandler( LdbcSnbBiQuery18.class, LdbcQuery18Handler.class );
        registerOperationHandler( LdbcSnbBiQuery19.class, LdbcQuery19Handler.class );
        registerOperationHandler( LdbcSnbBiQuery20.class, LdbcQuery20Handler.class );
        registerOperationHandler( LdbcSnbBiQuery21.class, LdbcQuery21Handler.class );
        registerOperationHandler( LdbcSnbBiQuery22.class, LdbcQuery22Handler.class );
        registerOperationHandler( LdbcSnbBiQuery23.class, LdbcQuery23Handler.class );
        registerOperationHandler( LdbcSnbBiQuery24.class, LdbcQuery24Handler.class );
    }

    @Override
    protected void onClose() throws IOException
    {
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException
    {
        return null;
    }

    private static void sleep( long sleepNs )
    {
        sleepFun.sleep( sleepNs );
    }

    /*
    LONG READS
     */

    private static final List<LdbcSnbBiQuery1Result> LDBC_QUERY_1_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read1Results();

    public static class LdbcQuery1Handler implements OperationHandler<LdbcSnbBiQuery1,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery1 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_1_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery2Result> LDBC_QUERY_2_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read2Results();

    public static class LdbcQuery2Handler implements OperationHandler<LdbcSnbBiQuery2,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery2 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_2_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery3Result> LDBC_QUERY_3_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read3Results();

    public static class LdbcQuery3Handler implements OperationHandler<LdbcSnbBiQuery3,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery3 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_3_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery4Result> LDBC_QUERY_4_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read4Results();

    public static class LdbcQuery4Handler implements OperationHandler<LdbcSnbBiQuery4,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery4 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_4_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery5Result> LDBC_QUERY_5_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read5Results();

    public static class LdbcQuery5Handler implements OperationHandler<LdbcSnbBiQuery5,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery5 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_5_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery6Result> LDBC_QUERY_6_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read6Results();

    public static class LdbcQuery6Handler implements OperationHandler<LdbcSnbBiQuery6,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery6 operation, DummyDbConnectionState dummyDbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_6_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery7Result> LDBC_QUERY_7_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read7Results();

    public static class LdbcQuery7Handler implements OperationHandler<LdbcSnbBiQuery7,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery7 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_7_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery8Result> LDBC_QUERY_8_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read8Results();

    public static class LdbcQuery8Handler implements OperationHandler<LdbcSnbBiQuery8,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery8 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_8_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery9Result> LDBC_QUERY_9_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read9Results();

    public static class LdbcQuery9Handler implements OperationHandler<LdbcSnbBiQuery9,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery9 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_9_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery10Result> LDBC_QUERY_10_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read10Results();

    public static class LdbcQuery10Handler implements OperationHandler<LdbcSnbBiQuery10,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery10 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_10_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery11Result> LDBC_QUERY_11_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read11Results();

    public static class LdbcQuery11Handler implements OperationHandler<LdbcSnbBiQuery11,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery11 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_11_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery12Result> LDBC_QUERY_12_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read12Results();

    public static class LdbcQuery12Handler implements OperationHandler<LdbcSnbBiQuery12,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery12 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_12_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery13Result> LDBC_QUERY_13_RESULTS =
            DummyLdbcSnbBiOperationResultSets.read13Results();

    public static class LdbcQuery13Handler implements OperationHandler<LdbcSnbBiQuery13,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery13 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_13_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery14Result> LDBC_QUERY_14_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read14Results();

    public static class LdbcQuery14Handler implements OperationHandler<LdbcSnbBiQuery14,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery14 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_14_RESULTS, operation );
        }
    }

    // TODO

    private static final List<LdbcSnbBiQuery15Result> LDBC_QUERY_15_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read15Results();

    public static class LdbcQuery15Handler implements OperationHandler<LdbcSnbBiQuery15,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery15 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_15_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery16Result> LDBC_QUERY_16_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read16Results();

    public static class LdbcQuery16Handler implements OperationHandler<LdbcSnbBiQuery16,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery16 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_16_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery17Result> LDBC_QUERY_17_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read17Results();

    public static class LdbcQuery17Handler implements OperationHandler<LdbcSnbBiQuery17,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery17 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_17_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery18Result> LDBC_QUERY_18_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read18Results();

    public static class LdbcQuery18Handler implements OperationHandler<LdbcSnbBiQuery18,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery18 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_18_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery19Result> LDBC_QUERY_19_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read19Results();

    public static class LdbcQuery19Handler implements OperationHandler<LdbcSnbBiQuery19,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery19 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_19_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery20Result> LDBC_QUERY_20_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read20Results();

    public static class LdbcQuery20Handler implements OperationHandler<LdbcSnbBiQuery20,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery20 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_20_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery21Result> LDBC_QUERY_21_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read21Results();

    public static class LdbcQuery21Handler implements OperationHandler<LdbcSnbBiQuery21,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery21 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_21_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery22Result> LDBC_QUERY_22_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read22Results();

    public static class LdbcQuery22Handler implements OperationHandler<LdbcSnbBiQuery22,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery22 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_22_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery23Result> LDBC_QUERY_23_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read23Results();

    public static class LdbcQuery23Handler implements OperationHandler<LdbcSnbBiQuery23,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery23 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_23_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery24Result> LDBC_QUERY_24_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read24Results();

    public static class LdbcQuery24Handler implements OperationHandler<LdbcSnbBiQuery24,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery24 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_24_RESULTS, operation );
        }
    }
}
