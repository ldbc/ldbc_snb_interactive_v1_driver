package com.yahoo.ycsb;

import java.text.DecimalFormat;
import java.util.Vector;

import com.yahoo.ycsb.measurements.Measurements;

/**
 * Thread periodically displays benchmark status to show if progress being made
 * 
 * @author cooperb
 */
class StatusThread extends Thread
{
    final long statusReportingInterval = 10000;
    final Vector<ClientThread> clientThreads;
    String label;
    boolean standardStatus;

    public StatusThread( Vector<ClientThread> clientThreads, String label, boolean standardStatus )
    {
        this.clientThreads = clientThreads;
        this.label = label;
        this.standardStatus = standardStatus;
    }

    // Periodically report status
    public void run()
    {
        long startTime = System.currentTimeMillis();

        long elapsedTime = 0;
        long lastElapsedTime = elapsedTime;

        long totalOperations = 0;
        long lastTotalOperations = totalOperations;

        boolean allDone = true;

        do
        {
            allDone = true;
            totalOperations = 0;

            // terminate thread when all client threads are done
            for ( ClientThread clientThread : clientThreads )
            {
                if ( clientThread.getState() != Thread.State.TERMINATED )
                {
                    allDone = false;
                }
                totalOperations += clientThread.getOpsDone();
            }

            elapsedTime = System.currentTimeMillis() - startTime;

            double currentThroughput = 1000.0 * ( ( (double) ( totalOperations - lastTotalOperations ) ) / ( (double) ( elapsedTime - lastElapsedTime ) ) );

            lastTotalOperations = totalOperations;
            lastElapsedTime = elapsedTime;

            DecimalFormat decimalFormat = new DecimalFormat( "#.##" );

            if ( totalOperations == 0 )
            {
                // TODO use proper logger, why is .err. used?
                System.err.println( label + " " + ( elapsedTime / 1000 ) + " sec: " + totalOperations + " operations; "
                                    + Measurements.getMeasurements().getSummary() );
            }
            else
            {
                // TODO use proper logger, why is .err. used?
                System.err.println( label + " " + ( elapsedTime / 1000 ) + " sec: " + totalOperations + " operations; "
                                    + decimalFormat.format( currentThroughput ) + " current ops/sec; "
                                    + Measurements.getMeasurements().getSummary() );
            }

            if ( standardStatus )
            {
                if ( totalOperations == 0 )
                {
                    System.out.println( label + " " + ( elapsedTime / 1000 ) + " sec: " + totalOperations
                                        + " operations; " + Measurements.getMeasurements().getSummary() );
                }
                else
                {
                    System.out.println( label + " " + ( elapsedTime / 1000 ) + " sec: " + totalOperations
                                        + " operations; " + decimalFormat.format( currentThroughput )
                                        + " current ops/sec; " + Measurements.getMeasurements().getSummary() );
                }
            }

            try
            {
                sleep( statusReportingInterval );
            }
            catch ( InterruptedException e )
            {
                // do nothing
            }

        }
        while ( !allDone );
    }
}
