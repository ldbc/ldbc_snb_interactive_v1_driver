/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
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

package com.yahoo.ycsb;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yahoo.ycsb.generator.GeneratorFactory;

/**
 * One experiment scenario. One object of this type will be instantiated and
 * shared among all client threads. This class should be constructed using a
 * no-argument constructor, so we can load it dynamically. Any argument-based
 * initialization should be done by init().
 * 
 * If you extend this class, you should support the "insertstart" property. This
 * allows the load phase to proceed from multiple clients on different machines,
 * in case the client is the bottleneck. For example, if we want to load 1
 * million records from 2 machines, the first machine should have insertstart=0
 * and the second insertstart=500000. Additionally, the "insertcount" property,
 * which is interpreted by Client, can be used to tell each instance of the
 * client how many inserts to do. In the example above, both clients should have
 * insertcount=500000.
 */
public abstract class Workload
{
    public static final String INSERT_START = "insertstart";
    public static final String INSERT_START_DEFAULT = "0";

    private volatile AtomicBoolean stopRequested = new AtomicBoolean( false );

    /*
     * Initialize the benchmark scenario: create generators and shared objects. 
     * Called once, by main Client, at the beginning of a benchmark execution.
     */
    public void init( Properties p, GeneratorFactory generatorFactory ) throws WorkloadException
    {
    }

    // TODO this seems back the front, default should be local state
    // TODO make all state local, with ability to specify shared state
    // TODO don't use object, create a class for this
    /*
     * Initialize any state for a particular ClientThread 
     * 
     * One Workload instance is shared among all ClientThreads instances, 
     * this is the place to create any state that is specific to one thread. 
     * 
     * Return new instance on each call; DO NOT return same object multiple times. 
     * Object will be passed to invocations of doInsert()/doTransaction() for this thread.
     * 
     * There should be no side effects from this call; all state should be
     * encapsulated in the returned object. 
     * 
     * If no thread-specific state to retain, return null. 
     * 
     * @return false if the workload knows it is done for this thread. Client
     *         will terminate the thread. Return true otherwise. Return true for
     *         workloads that rely on operationcount. For workloads that read
     *         traces from a file, return true when there are more to do, false
     *         when you are done.
     */
    public Object initThread( Properties p, int myThreadId, int threadCount ) throws WorkloadException
    {
        return null;
    }

    /*
     * Called once by main Client after all operations have completed
     */
    public void cleanup() throws WorkloadException
    {
    }

    // TODO consider removing and replacing with a "bulk load" phase
    /*
     * Perform one insert operation. 
     * Called concurrently from multiple ClientThread instances. 
     * DO NOT USE synchronize: threads will block and it will be difficult to reach target throughput. 
     * 
     * Must be thread safe - no side effects other than: 
     *  - DB operations 
     *  - threadState mutations (no need to synchronize, threadState is not shared)
     *  - shared Generator mutations (Generator.next() mutates internal Generator state)
     */
    public abstract boolean doInsert( DB db, Object threadState ) throws WorkloadException;

    /*
     * Perform one transaction operation. 
     * Called concurrently from multiple ClientThread instances.
     * DO NOT USE synchronize: threads will block and it will be difficult to reach target throughput.
     *
     * Must be thread safe - no side effects other than: 
     *  - DB operations 
     *  - threadState mutations (no need to synchronize, threadState is not shared)
     *  - shared Generator mutations (Generator.next() mutates internal Generator state)
     *  
     * Return false if Workload knows it is done for this thread - Client will terminate the thread.
     * Return true otherwise. 
     * 
     * For Workloads that rely on operationCount return true.
     * For workloads that read traces from a file, return true until nothing more to read.
     */
    public abstract boolean doTransaction( DB db, Object threadState ) throws WorkloadException;

    /*
     * Schedule a request to stop the Workload
     */
    public void requestStop()
    {
        stopRequested.set( true );
    }

    public boolean isStopRequested()
    {
        if ( stopRequested.get() == true )
            return true;
        else
            return false;
    }
}
