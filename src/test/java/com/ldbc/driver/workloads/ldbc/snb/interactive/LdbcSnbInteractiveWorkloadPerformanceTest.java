package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.HdrHistogram.Histogram;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class LdbcSnbInteractiveWorkloadPerformanceTest {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    TimeSource timeSource = new SystemTimeSource();

    @Ignore
    @Test
    public void hdrShouldNotTakeAllTheFuckingRam() {
        List<Histogram> histograms = new ArrayList<>();
        long estimatedBytes = 0;
        for (int i = 0; i < 100; i++) {
            Histogram histogram = new Histogram(1l, new TemporalUtil().convert(90, TimeUnit.MINUTES, TimeUnit.NANOSECONDS), 4);
            histograms.add(histogram);
            estimatedBytes += histogram.getEstimatedFootprintInBytes();
            long estimatedKb = estimatedBytes / 1024;
            long estimatedMb = estimatedKb / 1024;
            System.out.println("Estimated MB (cumulative): " + estimatedMb);
        }
    }

    /*
SF30 1,2,4 threads 1 partition
[TC1-social_network]Completed 100,000,000 operations in 02:35.771.000 (m:s.ms.us) = 641,968 op/sec = 1 op/1.55771 us
[TC2-social_network]Completed 100,000,000 operations in 02:35.916.000 (m:s.ms.us) = 641,371 op/sec = 1 op/1.55916 us
[TC4-social_network]Completed 100,000,000 operations in 02:15.686.000 (m:s.ms.us) = 736,996 op/sec = 1 op/1.35686 us
     */
    @Ignore
    @Test
    public void ignoreTimesPerformanceTest()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
//        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/new_read_params/");
//        List<File> streamsDirs = Lists.newArrayList(
//                new File(parentStreamsDir, "sf10_partitions_01/")
////                new File(parentStreamsDir, "sf10_partitions_04/")
////                new File(parentStreamsDir, "sf10_partitions_16/")
//        );

        File parentStreamsDir = new File("/Users/alexaverbuch/hadoopTempDir/output/social_network/");
        File paramsDir = new File("/Users/alexaverbuch/IdeaProjects/ldbc_snb_datagen/substitution_parameters/");
        List<File> streamsDirs = Lists.newArrayList(
                parentStreamsDir
        );


        for (File streamDir : streamsDirs) {
            Iterable<String> personFilePaths = Iterables.transform(
                    Lists.newArrayList(
                            streamDir.listFiles(
                                    new FilenameFilter() {
                                        @Override
                                        public boolean accept(File dir, String name) {
                                            return name.endsWith("_person.csv");
                                        }
                                    }
                            )
                    ),
                    new Function<File, String>() {
                        @Override
                        public String apply(File file) {
                            return file.getAbsolutePath();
                        }
                    }
            );
            Iterable<String> forumFilePaths = Iterables.transform(
                    Lists.newArrayList(
                            streamDir.listFiles(
                                    new FilenameFilter() {
                                        @Override
                                        public boolean accept(File dir, String name) {
                                            return name.endsWith("_forum.csv");
                                        }
                                    }
                            )
                    ),
                    new Function<File, String>() {
                        @Override
                        public String apply(File file) {
                            return file.getAbsolutePath();
                        }
                    }
            );

            List<Integer> threadCounts = Lists.newArrayList(1, 2, 4);
            long operationCount = 100000000;
            for (int threadCount : threadCounts) {
                doIgnoreTimesPerformanceTest(
                        threadCount,
                        operationCount,
                        forumFilePaths,
                        personFilePaths,
                        paramsDir.getAbsolutePath(),
                        streamDir.getAbsolutePath(),
                        "TC" + threadCount + "-" + streamDir.getName(),
                        new File(streamDir, "updateStream.properties").getAbsolutePath()
                );
            }
        }
    }

    public void doIgnoreTimesPerformanceTest(int threadCount,
                                             long operationCount,
                                             Iterable<String> forumFilePaths,
                                             Iterable<String> personFilePaths,
                                             String parametersDir,
                                             String resultsDir,
                                             String name,
                                             String updateStreamPropertiesPath)
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentControlService controlService = null;
        Db db = null;
        Workload workload = null;
        ConcurrentMetricsService metricsService = null;
        try {
            Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultConfig();
            paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, parametersDir);
            paramsMap.put(LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(forumFilePaths));
            paramsMap.put(LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(personFilePaths));
            paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATE_STREAM_PARSER, LdbcSnbInteractiveConfiguration.UpdateStreamParser.CHAR_SEEKER.name());
            paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_MILLI_ARG, "0");
            // Driver-specific parameters
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 2;
            TimeUnit timeUnit = TimeUnit.MICROSECONDS;
            String resultDirPath = resultsDir;
            double timeCompressionRatio = 0.0000001;
            long windowedExecutionWindowDuration = 1000l;
            Set<String> peerIds = new HashSet<>();
            long toleratedExecutionDelay = TEMPORAL_UTIL.convert(60, TimeUnit.MINUTES, TimeUnit.MILLISECONDS);
            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
            String dbValidationFilePath = null;
            boolean validateWorkload = false;
            boolean calculateWorkloadStatistics = false;
            long spinnerSleepDuration = 0;
            boolean printHelp = false;
            boolean ignoreScheduledStartTimes = true;
            boolean shouldCreateResultsLog = true;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                    paramsMap,
                    name,
                    dbClassName,
                    workloadClassName,
                    operationCount,
                    threadCount,
                    statusDisplayInterval,
                    timeUnit,
                    resultDirPath,
                    timeCompressionRatio,
                    windowedExecutionWindowDuration,
                    peerIds,
                    toleratedExecutionDelay,
                    validationParams,
                    dbValidationFilePath,
                    validateWorkload,
                    calculateWorkloadStatistics,
                    spinnerSleepDuration,
                    printHelp,
                    ignoreScheduledStartTimes,
                    shouldCreateResultsLog
            );

            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(MapUtils.loadPropertiesToMap(new File(updateStreamPropertiesPath)));

            // When
            Client client = new Client(new LocalControlService(timeSource.nowAsMilli() + 3000, configuration), timeSource);
            client.start();

            // Then
            File resultsFile = new File(resultsDir, name + "-results.json");
            WorkloadResultsSnapshot resultsSnapshot = WorkloadResultsSnapshot.fromJson(resultsFile);

            double operationsPerSecond = Math.round(((double) operationCount / resultsSnapshot.totalRunDurationAsNano()) * TEMPORAL_UTIL.convert(1, TimeUnit.SECONDS, TimeUnit.NANOSECONDS));
            double microSecondPerOperation = (double) TEMPORAL_UTIL.convert(resultsSnapshot.totalRunDurationAsNano(), TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS) / operationCount;
            DecimalFormat numberFormatter = new DecimalFormat("###,###,###,###");
            System.out.println(
                    String.format("[%s]Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            name,
                            numberFormatter.format(operationCount),
                            TEMPORAL_UTIL.nanoDurationToString(resultsSnapshot.totalRunDurationAsNano()),
                            numberFormatter.format(operationsPerSecond),
                            microSecondPerOperation
                    )
            );
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (errorReporter.errorEncountered())
                System.out.println(errorReporter.toString());
            if (null != controlService) controlService.shutdown();
            if (null != db) db.close();
            if (null != workload) workload.close();
            if (null != metricsService) metricsService.shutdown();
        }
    }


    /*
    ********************************************************
************ Calculated Workload Statistics ************
********************************************************
  ------------------------------------------------------
  GENERAL
  ------------------------------------------------------
     Operation Count:                   100,000,000
     Unique Operation Types:            22
     Total Duration:                    01:21.716.000 (m:s.ms.us)
     Time Span:                         2014-10-30 - 11:10:05.546, 2014-10-30 - 11:11:27.262
     Operation Mix:
        LdbcQuery10:                    2,017,680
        LdbcQuery11:                    29,584
        LdbcQuery12:                    585,778
        LdbcQuery13:                    445,076
        LdbcQuery14:                    832,987
        LdbcQuery1:                     442,905
        LdbcQuery2:                     223,634
        LdbcQuery3:                     2,161,800
        LdbcQuery4:                     14,478
        LdbcQuery5:                     507,238
        LdbcQuery6:                     5,044,201
        LdbcQuery7:                     9,831
        LdbcQuery8:                     2,982
        LdbcQuery9:                     4,778,717
        LdbcUpdate1AddPerson:           15,697
        LdbcUpdate2AddPostLike:         12,259,853
        LdbcUpdate3AddCommentLike:      13,480,471
        LdbcUpdate4AddForum:            285,310
        LdbcUpdate5AddForumMembership:  42,432,546
        LdbcUpdate6AddPost:             3,493,189
        LdbcUpdate7AddComment:          8,547,315
        LdbcUpdate8AddFriendship:       2,388,728
     Operation By Dependency Mode:
        All Operations:                 [LdbcQuery1, LdbcQuery10, LdbcQuery11, LdbcQuery12, LdbcQuery13, LdbcQuery14, LdbcQuery2, LdbcQuery3, LdbcQuery4, LdbcQuery5, LdbcQuery6, LdbcQuery7, LdbcQuery8, LdbcQuery9, LdbcUpdate1AddPerson, LdbcUpdate2AddPostLike, LdbcUpdate3AddCommentLike, LdbcUpdate4AddForum, LdbcUpdate5AddForumMembership, LdbcUpdate6AddPost, LdbcUpdate7AddComment, LdbcUpdate8AddFriendship]
        Dependency Operations:          [LdbcUpdate1AddPerson, LdbcUpdate8AddFriendship]
        Dependent Operations:           [LdbcUpdate1AddPerson, LdbcUpdate2AddPostLike, LdbcUpdate3AddCommentLike, LdbcUpdate4AddForum, LdbcUpdate5AddForumMembership, LdbcUpdate6AddPost, LdbcUpdate7AddComment, LdbcUpdate8AddFriendship]
  ------------------------------------------------------
  INTERLEAVES
  ------------------------------------------------------
        All Operations:                 min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us)
        Dependency Operations:          min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us)
        Dependent Operations:           min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us)
  ------------------------------------------------------
  BY OPERATION TYPE
  ------------------------------------------------------
     LdbcQuery10:                       Min Dependency Duration(1620) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcQuery11:                       Min Dependency Duration(1623) Time Span(11:10:05.549, 11:11:27.260) Interleave(min = 00:00.002.000 (m:s.ms.us) / mean = 00:00.003.000 (m:s.ms.us) / max = 00:00.003.000 (m:s.ms.us))
     LdbcQuery12:                       Min Dependency Duration(1620) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcQuery13:                       Min Dependency Duration(1620) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcQuery14:                       Min Dependency Duration(1620) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcQuery1:                        Min Dependency Duration(1620) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcQuery2:                        Min Dependency Duration(1620) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcQuery3:                        Min Dependency Duration(1620) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcQuery4:                        Min Dependency Duration(1626) Time Span(11:10:05.552, 11:11:27.258) Interleave(min = 00:00.005.000 (m:s.ms.us) / mean = 00:00.006.000 (m:s.ms.us) / max = 00:00.006.000 (m:s.ms.us))
     LdbcQuery5:                        Min Dependency Duration(1620) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcQuery6:                        Min Dependency Duration(1620) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcQuery7:                        Min Dependency Duration(1628) Time Span(11:10:05.554, 11:11:27.256) Interleave(min = 00:00.008.000 (m:s.ms.us) / mean = 00:00.008.000 (m:s.ms.us) / max = 00:00.009.000 (m:s.ms.us))
     LdbcQuery8:                        Min Dependency Duration(1647) Time Span(11:10:05.573, 11:11:27.254) Interleave(min = 00:00.027.000 (m:s.ms.us) / mean = 00:00.027.000 (m:s.ms.us) / max = 00:00.028.000 (m:s.ms.us))
     LdbcQuery9:                        Min Dependency Duration(1620) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcUpdate1AddPerson:              Min Dependency Duration(10000) Time Span(11:10:05.548, 11:11:27.255) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.005.000 (m:s.ms.us) / max = 00:00.046.000 (m:s.ms.us))
     LdbcUpdate2AddPostLike:            Min Dependency Duration(10000) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcUpdate3AddCommentLike:         Min Dependency Duration(10000) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcUpdate4AddForum:               Min Dependency Duration(10000) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.004.000 (m:s.ms.us))
     LdbcUpdate5AddForumMembership:     Min Dependency Duration(10000) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcUpdate6AddPost:                Min Dependency Duration(10000) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.002.000 (m:s.ms.us))
     LdbcUpdate7AddComment:             Min Dependency Duration(10000) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
     LdbcUpdate8AddFriendship:          Min Dependency Duration(10000) Time Span(11:10:05.546, 11:11:27.262) Interleave(min = 00:00.000.000 (m:s.ms.us) / mean = 00:00.000.000 (m:s.ms.us) / max = 00:00.001.000 (m:s.ms.us))
********************************************************
     */
    @Ignore
    @Test
    public void withTimesPerformanceTest()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
//        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/new_read_params/");
//        List<File> streamsDirs = Lists.newArrayList(
//                new File(parentStreamsDir, "sf10_partitions_01/")
////                new File(parentStreamsDir, "sf10_partitions_04/")
////                new File(parentStreamsDir, "sf10_partitions_16/")
//        );

        File parentStreamsDir = new File("/Users/alexaverbuch/hadoopTempDir/output/social_network/");
        File paramsDir = new File("/Users/alexaverbuch/IdeaProjects/ldbc_snb_datagen/substitution_parameters/");
        List<File> streamsDirs = Lists.newArrayList(
                parentStreamsDir
        );


        for (File streamDir : streamsDirs) {
            Iterable<String> personFilePaths = Iterables.transform(
                    Lists.newArrayList(
                            streamDir.listFiles(
                                    new FilenameFilter() {
                                        @Override
                                        public boolean accept(File dir, String name) {
                                            return name.endsWith("_person.csv");
                                        }
                                    }
                            )
                    ),
                    new Function<File, String>() {
                        @Override
                        public String apply(File file) {
                            return file.getAbsolutePath();
                        }
                    }
            );
            Iterable<String> forumFilePaths = Iterables.transform(
                    Lists.newArrayList(
                            streamDir.listFiles(
                                    new FilenameFilter() {
                                        @Override
                                        public boolean accept(File dir, String name) {
                                            return name.endsWith("_forum.csv");
                                        }
                                    }
                            )
                    ),
                    new Function<File, String>() {
                        @Override
                        public String apply(File file) {
                            return file.getAbsolutePath();
                        }
                    }
            );

            List<Integer> threadCounts = Lists.newArrayList(1, 2, 4);
            long operationCount = 100000000;
            for (int threadCount : threadCounts) {
                doWithTimesPerformanceTest(
                        threadCount,
                        operationCount,
                        forumFilePaths,
                        personFilePaths,
                        paramsDir.getAbsolutePath(),
                        streamDir.getAbsolutePath(),
                        "TC" + threadCount + "-" + streamDir.getName(),
                        new File(streamDir, "updateStream.properties").getAbsolutePath()
                );
            }
        }
    }

    public void doWithTimesPerformanceTest(int threadCount,
                                           long operationCount,
                                           Iterable<String> forumFilePaths,
                                           Iterable<String> personFilePaths,
                                           String parametersDir,
                                           String resultsDir,
                                           String name,
                                           String updateStreamPropertiesPath)
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentControlService controlService = null;
        Db db = null;
        Workload workload = null;
        ConcurrentMetricsService metricsService = null;
        try {
            Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultConfig();
            paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, parametersDir);
            paramsMap.put(LdbcSnbInteractiveConfiguration.FORUM_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(forumFilePaths));
            paramsMap.put(LdbcSnbInteractiveConfiguration.PERSON_UPDATE_FILES, LdbcSnbInteractiveConfiguration.serializeFilePathsListFromConfiguration(personFilePaths));
            paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATE_STREAM_PARSER, LdbcSnbInteractiveConfiguration.UpdateStreamParser.CHAR_SEEKER.name());
            paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_MILLI_ARG, "0");
            // Driver-specific parameters
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 2;
            TimeUnit timeUnit = TimeUnit.MICROSECONDS;
            String resultDirPath = resultsDir;
            double timeCompressionRatio = 0.00001;
            long windowedExecutionWindowDuration = 1000l;
            Set<String> peerIds = new HashSet<>();
            long toleratedExecutionDelay = TEMPORAL_UTIL.convert(60, TimeUnit.MINUTES, TimeUnit.MILLISECONDS);
            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
            String dbValidationFilePath = null;
            boolean validateWorkload = false;
            boolean calculateWorkloadStatistics = true;
            long spinnerSleepDuration = 0;
            boolean printHelp = false;
            boolean ignoreScheduledStartTimes = false;
            boolean shouldCreateResultsLog = true;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                    paramsMap,
                    name,
                    dbClassName,
                    workloadClassName,
                    operationCount,
                    threadCount,
                    statusDisplayInterval,
                    timeUnit,
                    resultDirPath,
                    timeCompressionRatio,
                    windowedExecutionWindowDuration,
                    peerIds,
                    toleratedExecutionDelay,
                    validationParams,
                    dbValidationFilePath,
                    validateWorkload,
                    calculateWorkloadStatistics,
                    spinnerSleepDuration,
                    printHelp,
                    ignoreScheduledStartTimes,
                    shouldCreateResultsLog
            );

            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(MapUtils.loadPropertiesToMap(new File(updateStreamPropertiesPath)));

            // When
            Client client = new Client(new LocalControlService(timeSource.nowAsMilli() + 3000, configuration), timeSource);
            client.start();

            // Then
            File resultsFile = new File(resultsDir, name + "-results.json");
            WorkloadResultsSnapshot resultsSnapshot = WorkloadResultsSnapshot.fromJson(resultsFile);

            double operationsPerSecond = Math.round(((double) operationCount / resultsSnapshot.totalRunDurationAsNano()) * TEMPORAL_UTIL.convert(1, TimeUnit.SECONDS, TimeUnit.NANOSECONDS));
            double microSecondPerOperation = (double) TEMPORAL_UTIL.convert(resultsSnapshot.totalRunDurationAsNano(), TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS) / operationCount;
            DecimalFormat numberFormatter = new DecimalFormat("###,###,###,###");
            System.out.println(
                    String.format("[%s]Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            name,
                            numberFormatter.format(operationCount),
                            TEMPORAL_UTIL.nanoDurationToString(resultsSnapshot.totalRunDurationAsNano()),
                            numberFormatter.format(operationsPerSecond),
                            microSecondPerOperation
                    )
            );
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (errorReporter.errorEncountered())
                System.out.println(errorReporter.toString());
            if (null != controlService) controlService.shutdown();
            if (null != db) db.close();
            if (null != workload) workload.close();
            if (null != metricsService) metricsService.shutdown();
        }
    }
}
