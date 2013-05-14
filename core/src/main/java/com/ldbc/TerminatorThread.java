/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.
 */
package com.ldbc;

import java.util.Vector;

import com.ldbc.workloads.Workload;

/**
 * Terminates all client threads after maximum specified time has passed
 * 
 * @author sudipto
 * 
 */
public class TerminatorThread extends Thread
{

    private final Vector<ClientThread> clientThreads;
    private final long maxExecutionTimeSeconds;
    private final Workload workload;
    private final long waitTimeoutInMS;

    /**
     * @param maxExecutionTime (seconds)
     * @param clientThreads
     * @param workload
     */
    public TerminatorThread( long maxExecutionTime, Vector<ClientThread> clientThreads, Workload workload )
    {
        this.maxExecutionTimeSeconds = maxExecutionTime;
        this.clientThreads = clientThreads;
        this.workload = workload;
        this.waitTimeoutInMS = 2000;
        System.err.println( "Maximum execution time specified as: " + maxExecutionTime + " seconds" );
    }

    public void run()
    {
        try
        {
            Thread.sleep( maxExecutionTimeSeconds * 1000 );
        }
        catch ( InterruptedException e )
        {
            System.err.println( "Could not wait until max specified time, TerminatorThread interrupted" );
            return;
        }
        System.err.println( "Maximum time elapsed, requesting stop for the workload" );
        workload.requestStop();
        System.err.println( "Stop requested for workload. Now Joining!" );
        for ( ClientThread clientThread : clientThreads )
        {
            while ( clientThread.isAlive() )
            {
                try
                {
                    clientThread.join( waitTimeoutInMS );
                    if ( clientThread.isAlive() )
                    {
                        System.err.println( "Still waiting for thread " + clientThread.getName() + " to complete. "
                                            + "Workload status: " + workload.isStopRequested() );
                    }
                }
                catch ( InterruptedException e )
                {
                    // TODO message below is suspect, maybe throw exception here
                    // Do nothing. Don't know why I was interrupted.
                }
            }
        }
    }
}
