package org.ldbcouncil.snb.driver.workloads.simple;

import com.google.common.collect.Lists;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.Workload;
import org.ldbcouncil.snb.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.snb.driver.control.DriverConfiguration;
import org.ldbcouncil.snb.driver.util.Tuple;
import org.ldbcouncil.snb.driver.util.Tuple2;
import org.ldbcouncil.snb.driver.workloads.ClassNameWorkloadFactory;
import org.ldbcouncil.snb.driver.workloads.WorkloadTest;
import org.ldbcouncil.snb.driver.workloads.simple.db.SimpleDb;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

// TODO unignore and fix existing failures
@Ignore
public class SimpleWorkloadTest extends WorkloadTest
{
    @Override
    public Workload workload() throws Exception
    {
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(
                SimpleDb.class.getName(),
                SimpleWorkload.class.getName(),
                1_000_000
        ).applyArg(
                ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG,
                Long.toString( 1_000_000 )
        ).applyArg(
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                "true"
        );
        Workload workload = new ClassNameWorkloadFactory( configuration.workloadClassName() ).createWorkload();
        workload.init( configuration );
        return workload;
    }

    @Override
    public List<Tuple2<Operation,Object>> operationsAndResults() throws Exception
    {
        return Lists.newArrayList(
                Tuple.<Operation,Object>tuple2(
                        new InsertOperation(
                                "table",
                                "key",
                                new HashMap<String,Iterator<Byte>>()
                        ),
                        "nothing"
                ),
                Tuple.<Operation,Object>tuple2(
                        new ReadModifyWriteOperation(
                                "table",
                                "key",
                                new ArrayList<String>(),
                                new HashMap<String,Iterator<Byte>>()
                        ),
                        "nothing"
                ),
                Tuple.<Operation,Object>tuple2(
                        new ReadOperation(
                                "table",
                                "key",
                                new ArrayList<String>()
                        ),
                        "nothing"
                ),
                Tuple.<Operation,Object>tuple2(
                        new ScanOperation(
                                "table",
                                "startKey",
                                1,
                                new ArrayList<String>()
                        ),
                        "nothing"
                ),
                Tuple.<Operation,Object>tuple2(
                        new UpdateOperation(
                                "table",
                                "key",
                                new HashMap<String,Iterator<Byte>>()
                        ),
                        "nothing"
                )
        );
    }

    @Override
    public List<DriverConfiguration> configurations() throws Exception
    {
        return Lists.newArrayList(
                ConsoleAndFileDriverConfiguration.fromDefaults(
                        SimpleDb.class.getName(),
                        SimpleWorkload.class.getName(),
                        1_000_000
                ).applyArg(
                        ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG,
                        Long.toString( 1_000_000 )
                ).applyArg(
                        ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                        "false"
                ).applyArg(
                        ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG,
                        "0.0001"
                )
        );
    }
}
