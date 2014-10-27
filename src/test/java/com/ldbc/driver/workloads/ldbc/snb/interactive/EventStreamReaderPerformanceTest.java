package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker.EventDecoder;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.csv.*;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EventStreamReaderPerformanceTest {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    TimeSource timeSource = new SystemTimeSource();
    DecimalFormat numberFormatter = new DecimalFormat("###,###,###,###");

    @Ignore
    @Test
    public void newReadParamsParsingPerformanceTest() throws IOException {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/new_read_params/sf10_partitions_01/");
        File paramsFile = new File(parentStreamsDir, "query_1_param.txt");
        EventDecoder<Object[]> decoder = new Query1EventStreamReader.Query1Decoder();
        int bufferSize = 2 * 1024 * 1024;
        {
            // warm up file system
            doBufferedReaderPerformanceTest(paramsFile, bufferSize);
        }

        long limit = 100000000;
        int repetitions = 4;
        {
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {
                SimpleCsvFileReader readOperation1FileReader = new SimpleCsvFileReader(
                        paramsFile,
                        LdbcSnbInteractiveConfiguration.PIPE_SEPARATOR_REGEX
                );
                Iterator<String[]> csvStreamReader = gf.limit(
                        gf.repeating(
                                readOperation1FileReader
                        ),
                        limit
                );
                lines += readingStreamPerformanceTest(csvStreamReader);
                readOperation1FileReader.close();
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            double linesPerSecond = Math.round(((double) lines / durationAsMilli) * 1000l);
            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            SimpleCsvFileReader.class.getSimpleName(),
                            TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                            numberFormatter.format(lines),
                            numberFormatter.format(linesPerSecond)
                    )
            );
        }

        {
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {
                CharSeeker charSeeker = new BufferedCharSeeker(new FileReader(paramsFile), bufferSize);
                Extractors extractors = new Extractors(';');
                Mark mark = new Mark();
                int columnDelimiter = '|';
                // skip headers - this file has 2 columns per row
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
                Iterator<Object[]> operation1StreamWithoutTimes = gf.limit(
                        gf.repeating(
                                new CsvEventStreamReaderBasicCharSeeker<>(
                                        charSeeker,
                                        extractors,
                                        mark,
                                        decoder,
                                        columnDelimiter
                                )
                        ),
                        limit
                );
                lines += readingStreamPerformanceTest(operation1StreamWithoutTimes);
                charSeeker.close();
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            double linesPerSecond = Math.round(((double) lines / durationAsMilli) * 1000l);
            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            CsvEventStreamReaderBasicCharSeeker.class.getSimpleName(),
                            TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                            numberFormatter.format(lines),
                            numberFormatter.format(linesPerSecond)
                    )
            );
        }

        {
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {
                CharSeeker charSeeker = new BufferedCharSeeker(new FileReader(paramsFile), bufferSize);
                Extractors extractors = new Extractors(';');
                Mark mark = new Mark();
                int columnDelimiter = '|';

                // skip headers - this file has 2 columns per row
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});

                Iterator<Object[]> query1Parameters = gf.limit(
                        gf.repeating(
                                new CsvEventStreamReaderBasicCharSeeker<>(
                                        charSeeker,
                                        extractors,
                                        mark,
                                        decoder,
                                        columnDelimiter
                                )
                        ),
                        limit
                );
                Iterator<Operation<?>> query1OperationsWithoutTimes = new Query1EventStreamReader(query1Parameters);
                lines += readingStreamPerformanceTest(query1OperationsWithoutTimes);
                charSeeker.close();
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            double linesPerSecond = Math.round(((double) lines / durationAsMilli) * 1000l);
            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            Query1EventStreamReader.class.getSimpleName(),
                            TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                            numberFormatter.format(lines),
                            numberFormatter.format(linesPerSecond)
                    )
            );
        }

        {
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {
                CharSeeker charSeeker = new BufferedCharSeeker(new FileReader(paramsFile), bufferSize);
                Extractors extractors = new Extractors(';');
                Mark mark = new Mark();
                int columnDelimiter = '|';

                // skip headers - this file has 2 columns per row
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});

                Iterator<Object[]> query1Parameters = gf.limit(
                        gf.repeating(
                                new CsvEventStreamReaderBasicCharSeeker<>(
                                        charSeeker,
                                        extractors,
                                        mark,
                                        decoder,
                                        columnDelimiter
                                )
                        ),
                        limit
                );
                Iterator<Operation<?>> query1OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query1EventStreamReader(query1Parameters)
                );

                lines += readingStreamPerformanceTest(query1OperationsWithTimes);
                charSeeker.close();
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            double linesPerSecond = Math.round(((double) lines / durationAsMilli) * 1000l);
            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            Query1EventStreamReader.class.getSimpleName(),
                            TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                            numberFormatter.format(lines),
                            numberFormatter.format(linesPerSecond)
                    )
            );
        }
    }

    @Ignore
    @Test
    public void newParseAndMergeAllReadOperationStreamsPerformanceTest() throws IOException {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/new_read_params/sf10_partitions_01/");

        long limit = 100000000;
        int bufferSize = 2 * 1024 * 1024;
        int repetitions = 1;
        Extractors extractors = new Extractors(';');
        int columnDelimiter = '|';
        {
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {

                EventDecoder<Object[]> decoder1 = new Query1EventStreamReader.Query1Decoder();
                CharSeeker charSeeker1 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_1_param.txt")), bufferSize);
                Mark mark1 = new Mark();
                // skip headers
                charSeeker1.seek(mark1, new int[]{columnDelimiter});
                charSeeker1.seek(mark1, new int[]{columnDelimiter});
                Iterator<Operation<?>> query1OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query1EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker1,
                                                extractors,
                                                mark1,
                                                decoder1,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder2 = new Query2EventStreamReader.Query2Decoder();
                CharSeeker charSeeker2 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_2_param.txt")), bufferSize);
                Mark mark2 = new Mark();
                // skip headers
                charSeeker2.seek(mark2, new int[]{columnDelimiter});
                charSeeker2.seek(mark2, new int[]{columnDelimiter});
                Iterator<Operation<?>> query2OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query2EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker2,
                                                extractors,
                                                mark2,
                                                decoder2,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder3 = new Query3EventStreamReader.Query3Decoder();
                CharSeeker charSeeker3 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_3_param.txt")), bufferSize);
                Mark mark3 = new Mark();
                // skip headers
                charSeeker3.seek(mark3, new int[]{columnDelimiter});
                charSeeker3.seek(mark3, new int[]{columnDelimiter});
                charSeeker3.seek(mark3, new int[]{columnDelimiter});
                charSeeker3.seek(mark3, new int[]{columnDelimiter});
                charSeeker3.seek(mark3, new int[]{columnDelimiter});
                Iterator<Operation<?>> query3OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query3EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker3,
                                                extractors,
                                                mark3,
                                                decoder3,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder4 = new Query4EventStreamReader.Query4Decoder();
                CharSeeker charSeeker4 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_4_param.txt")), bufferSize);
                Mark mark4 = new Mark();
                // skip headers
                charSeeker4.seek(mark4, new int[]{columnDelimiter});
                charSeeker4.seek(mark4, new int[]{columnDelimiter});
                charSeeker4.seek(mark4, new int[]{columnDelimiter});
                Iterator<Operation<?>> query4OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query4EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker4,
                                                extractors,
                                                mark4,
                                                decoder4,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder5 = new Query5EventStreamReader.Query5Decoder();
                CharSeeker charSeeker5 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_5_param.txt")), bufferSize);
                Mark mark5 = new Mark();
                // skip headers
                charSeeker5.seek(mark5, new int[]{columnDelimiter});
                charSeeker5.seek(mark5, new int[]{columnDelimiter});
                Iterator<Operation<?>> query5OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query5EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker5,
                                                extractors,
                                                mark5,
                                                decoder5,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder6 = new Query6EventStreamReader.Query6Decoder();
                CharSeeker charSeeker6 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_6_param.txt")), bufferSize);
                Mark mark6 = new Mark();
                // skip headers
                charSeeker6.seek(mark6, new int[]{columnDelimiter});
                charSeeker6.seek(mark6, new int[]{columnDelimiter});
                Iterator<Operation<?>> query6OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query6EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker6,
                                                extractors,
                                                mark6,
                                                decoder6,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder7 = new Query7EventStreamReader.Query7Decoder();
                CharSeeker charSeeker7 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_7_param.txt")), bufferSize);
                Mark mark7 = new Mark();
                // skip headers
                charSeeker7.seek(mark7, new int[]{columnDelimiter});
                Iterator<Operation<?>> query7OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query7EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker7,
                                                extractors,
                                                mark7,
                                                decoder7,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder8 = new Query8EventStreamReader.Query8Decoder();
                CharSeeker charSeeker8 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_8_param.txt")), bufferSize);
                Mark mark8 = new Mark();
                // skip headers
                charSeeker8.seek(mark8, new int[]{columnDelimiter});
                Iterator<Operation<?>> query8OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query8EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker8,
                                                extractors,
                                                mark8,
                                                decoder8,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder9 = new Query9EventStreamReader.Query9Decoder();
                CharSeeker charSeeker9 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_9_param.txt")), bufferSize);
                Mark mark9 = new Mark();
                // skip headers
                charSeeker9.seek(mark9, new int[]{columnDelimiter});
                charSeeker9.seek(mark9, new int[]{columnDelimiter});
                Iterator<Operation<?>> query9OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query9EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker9,
                                                extractors,
                                                mark9,
                                                decoder9,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder10 = new Query10EventStreamReader.Query10Decoder();
                CharSeeker charSeeker10 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_10_param.txt")), bufferSize);
                Mark mark10 = new Mark();
                // skip headers
                charSeeker10.seek(mark10, new int[]{columnDelimiter});
                charSeeker10.seek(mark10, new int[]{columnDelimiter});
                Iterator<Operation<?>> query10OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query10EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker10,
                                                extractors,
                                                mark10,
                                                decoder10,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder11 = new Query11EventStreamReader.Query11Decoder();
                CharSeeker charSeeker11 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_11_param.txt")), bufferSize);
                Mark mark11 = new Mark();
                // skip headers
                charSeeker11.seek(mark11, new int[]{columnDelimiter});
                charSeeker11.seek(mark11, new int[]{columnDelimiter});
                charSeeker11.seek(mark11, new int[]{columnDelimiter});
                Iterator<Operation<?>> query11OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query11EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker11,
                                                extractors,
                                                mark11,
                                                decoder11,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder12 = new Query12EventStreamReader.Query12Decoder();
                CharSeeker charSeeker12 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_12_param.txt")), bufferSize);
                Mark mark12 = new Mark();
                // skip headers
                charSeeker12.seek(mark12, new int[]{columnDelimiter});
                charSeeker12.seek(mark12, new int[]{columnDelimiter});
                Iterator<Operation<?>> query12OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query12EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker12,
                                                extractors,
                                                mark12,
                                                decoder12,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder13 = new Query13EventStreamReader.Query13Decoder();
                CharSeeker charSeeker13 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_13_param.txt")), bufferSize);
                Mark mark13 = new Mark();
                // skip headers
                charSeeker13.seek(mark13, new int[]{columnDelimiter});
                charSeeker13.seek(mark13, new int[]{columnDelimiter});
                Iterator<Operation<?>> query13OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query13EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker13,
                                                extractors,
                                                mark13,
                                                decoder13,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                EventDecoder<Object[]> decoder14 = new Query14EventStreamReader.Query14Decoder();
                CharSeeker charSeeker14 = new BufferedCharSeeker(new FileReader(new File(parentStreamsDir, "query_14_param.txt")), bufferSize);
                Mark mark14 = new Mark();
                // skip headers
                charSeeker14.seek(mark14, new int[]{columnDelimiter});
                charSeeker14.seek(mark14, new int[]{columnDelimiter});
                Iterator<Operation<?>> query14OperationsWithTimes = gf.assignStartTimes(
                        gf.incrementing(0l, 1l),
                        new Query14EventStreamReader(
                                gf.repeating(
                                        new CsvEventStreamReaderBasicCharSeeker<>(
                                                charSeeker14,
                                                extractors,
                                                mark14,
                                                decoder14,
                                                columnDelimiter
                                        )
                                )
                        )
                );

                lines += readingStreamPerformanceTest(
                        gf.limit(
                                gf.mergeSortOperationsByStartTime(
                                        query1OperationsWithTimes,
                                        query2OperationsWithTimes,
                                        query3OperationsWithTimes,
                                        query4OperationsWithTimes,
                                        query5OperationsWithTimes,
                                        query6OperationsWithTimes,
                                        query7OperationsWithTimes,
                                        query8OperationsWithTimes,
                                        query9OperationsWithTimes,
                                        query10OperationsWithTimes,
                                        query11OperationsWithTimes,
                                        query12OperationsWithTimes,
                                        query13OperationsWithTimes,
                                        query14OperationsWithTimes
                                ),
                                limit
                        )
                );

                charSeeker1.close();
                charSeeker2.close();
                charSeeker3.close();
                charSeeker4.close();
                charSeeker5.close();
                charSeeker6.close();
                charSeeker7.close();
                charSeeker8.close();
                charSeeker9.close();
                charSeeker10.close();
                charSeeker11.close();
                charSeeker12.close();
                charSeeker13.close();
                charSeeker14.close();
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            double linesPerSecond = Math.round(((double) lines / durationAsMilli) * 1000l);
            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            "Merged",
                            TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                            numberFormatter.format(lines),
                            numberFormatter.format(linesPerSecond)
                    )
            );
        }
    }

    @Ignore
    @Test
    public void forumCsvUpdateStreamReadingRegexParserProfileTest() throws IOException, InterruptedException {
        Thread.sleep(30000);
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/");
        File forumUpdateStream = new File(parentStreamsDir, "sf10_partitions_01/updateStream_0_0_forum.csv");

        long lines = 0;
        long startTimeAsMilli = timeSource.nowAsMilli();
        SimpleCsvFileReader simpleCsvFileReader = new SimpleCsvFileReader(forumUpdateStream, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(simpleCsvFileReader);
        lines += readingStreamPerformanceTest(writeEventStreamReader);
        simpleCsvFileReader.close();
        long endTimeAsMilli = timeSource.nowAsMilli();
        long durationAsMilli = (endTimeAsMilli - startTimeAsMilli);

        System.out.println(
                String.format("%s took %s to read %s line: %s lines/s",
                        WriteEventStreamReaderRegex.class.getSimpleName(),
                        TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                        numberFormatter.format(lines),
                        numberFormatter.format((double) lines / TEMPORAL_UTIL.convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
                )
        );
    }

    @Ignore
    @Test
    public void forumCsvUpdateStreamReadingCharSeekerParserProfileTest() throws IOException, InterruptedException {
        Thread.sleep(30000);
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/current/");
        File forumUpdateStream = new File(parentStreamsDir, "sf10_partitions_01/updateStream_0_0_forum.csv");

        int MB = 1024 * 1024;
        int bufferSize = 2 * MB;
        long lines = 0;
        long startTimeAsMilli = timeSource.nowAsMilli();
        CharSeeker charSeeker = new BufferedCharSeeker(new InputStreamReader(new FileInputStream(forumUpdateStream), Charsets.UTF_8), bufferSize);
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
        lines += readingStreamPerformanceTest(writeEventStreamReader);
        charSeeker.close();
        long endTimeAsMilli = timeSource.nowAsMilli();
        long durationAsMilli = (endTimeAsMilli - startTimeAsMilli);

        System.out.println(
                String.format("%s took %s to read %s line: %s lines/s",
                        WriteEventStreamReaderCharSeeker.class.getSimpleName() + "-" + bufferSize,
                        TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                        numberFormatter.format(lines),
                        numberFormatter.format((double) lines / TEMPORAL_UTIL.convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
                )
        );
    }

    @Ignore
    @Test
    public void personCsvUpdateStreamReadingRegexParserProfileTest() throws IOException, InterruptedException {
        Thread.sleep(30000);
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/");
        File forumUpdateStream = new File(parentStreamsDir, "sf10_partitions_01/updateStream_0_0_person.csv");

        long lines = 0;
        long startTimeAsMilli = timeSource.nowAsMilli();
        SimpleCsvFileReader simpleCsvFileReader = new SimpleCsvFileReader(forumUpdateStream, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(simpleCsvFileReader);
        lines += readingStreamPerformanceTest(writeEventStreamReader);
        simpleCsvFileReader.close();
        long endTimeAsMilli = timeSource.nowAsMilli();
        long durationAsMilli = (endTimeAsMilli - startTimeAsMilli);

        System.out.println(
                String.format("%s took %s to read %s line: %s lines/s",
                        WriteEventStreamReaderRegex.class.getSimpleName(),
                        TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                        numberFormatter.format(lines),
                        numberFormatter.format((double) lines / TEMPORAL_UTIL.convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
                )
        );
    }

    @Ignore
    @Test
    public void personCsvUpdateStreamReadingCharSeekerParserProfileTest() throws IOException, InterruptedException {
        Thread.sleep(30000);
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/");
        File forumUpdateStream = new File(parentStreamsDir, "sf10_partitions_01/updateStream_0_0_person.csv");

        int MB = 1024 * 1024;
        int bufferSize = 2 * MB;
        long lines = 0;
        long startTimeAsMilli = timeSource.nowAsMilli();
        CharSeeker charSeeker = new BufferedCharSeeker(new InputStreamReader(new FileInputStream(forumUpdateStream), Charsets.UTF_8), bufferSize);
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
        lines += readingStreamPerformanceTest(writeEventStreamReader);
        charSeeker.close();
        long endTimeAsMilli = timeSource.nowAsMilli();
        long durationAsMilli = (endTimeAsMilli - startTimeAsMilli);

        System.out.println(
                String.format("%s took %s to read %s line: %s lines/s",
                        WriteEventStreamReaderCharSeeker.class.getSimpleName() + "-" + bufferSize,
                        TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                        numberFormatter.format(lines),
                        numberFormatter.format((double) lines / TEMPORAL_UTIL.convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
                )
        );
    }

    @Ignore
    @Test
    public void forumCsvUpdateStreamReadingPerformanceTest() throws IOException {
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/current/");
        File forumUpdateStream = new File(parentStreamsDir, "sf10_partitions_01/updateStream_0_0_forum.csv");

        {
            // warm up file system
            SimpleCsvFileReader simpleCsvFileReader = new SimpleCsvFileReader(forumUpdateStream, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
            readingStreamPerformanceTest(simpleCsvFileReader);
            simpleCsvFileReader.close();
        }

        int repetitions = 2;
        {
            int bufferSize = 2 * 1024 * 1024;
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {
                lines += doBufferedReaderPerformanceTest(forumUpdateStream, bufferSize);
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            BufferedReader.class.getSimpleName() + "-" + bufferSize,
                            TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                            numberFormatter.format(lines),
                            numberFormatter.format((double) lines / TEMPORAL_UTIL.convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
                    )
            );
        }

        {
            int bufferSize = 2 * 1024 * 1024;
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {
                lines += doCharSeekerPerformanceTest(forumUpdateStream, bufferSize);
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            CharSeeker.class.getSimpleName() + "-" + bufferSize,
                            TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                            numberFormatter.format(lines),
                            numberFormatter.format((double) lines / TEMPORAL_UTIL.convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
                    )
            );
        }

        {
            int bufferSize = 2 * 1024 * 1024;
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {
                lines += doThreadedCharSeekerPerformanceTest(forumUpdateStream, bufferSize);
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            CharSeeker.class.getSimpleName() + "-" + ThreadAheadReadable.class.getSimpleName() + "-" + bufferSize,
                            TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                            numberFormatter.format(lines),
                            numberFormatter.format((double) lines / TEMPORAL_UTIL.convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
                    )
            );
        }

        {
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {
                SimpleCsvFileReader simpleCsvFileReader = new SimpleCsvFileReader(forumUpdateStream, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
                lines += readingStreamPerformanceTest(simpleCsvFileReader);
                simpleCsvFileReader.close();
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            SimpleCsvFileReader.class.getSimpleName(),
                            TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                            numberFormatter.format(lines),
                            numberFormatter.format((double) lines / TEMPORAL_UTIL.convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
                    )
            );
        }

        {
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {
                SimpleCsvFileReader simpleCsvFileReader = new SimpleCsvFileReader(forumUpdateStream, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
                WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(simpleCsvFileReader);
                lines += readingStreamPerformanceTest(writeEventStreamReader);
                simpleCsvFileReader.close();
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            WriteEventStreamReaderRegex.class.getSimpleName(),
                            TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                            numberFormatter.format(lines),
                            numberFormatter.format((double) lines / TEMPORAL_UTIL.convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
                    )
            );
        }

        int MB = 1024 * 1024;
        List<Integer> bufferSizes = Lists.newArrayList(1 * MB, 2 * MB, 4 * MB, 8 * MB, 16 * MB);
        for (int bufferSize : bufferSizes) {
            {
                long lines = 0;
                long startTimeAsMilli = timeSource.nowAsMilli();
                for (int i = 0; i < repetitions; i++) {
                    CharSeeker charSeeker = new BufferedCharSeeker(new InputStreamReader(new FileInputStream(forumUpdateStream), Charsets.UTF_8), bufferSize);
                    int columnDelimiter = '|';
                    Extractors extractors = new Extractors(';');
                    WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
                    lines += readingStreamPerformanceTest(writeEventStreamReader);
                    charSeeker.close();
                }
                long endTimeAsMilli = timeSource.nowAsMilli();
                long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
                lines = lines / repetitions;

                System.out.println(
                        String.format("%s took %s to read %s line: %s lines/s",
                                WriteEventStreamReaderCharSeeker.class.getSimpleName() + "-" + bufferSize,
                                TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                                numberFormatter.format(lines),
                                numberFormatter.format((double) lines / TEMPORAL_UTIL.convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
                        )
                );
            }

            {
                long lines = 0;
                long startTimeAsMilli = timeSource.nowAsMilli();
                for (int i = 0; i < repetitions; i++) {
                    Readable readable = new InputStreamReader(new FileInputStream(forumUpdateStream), Charsets.UTF_8);
                    CharSeeker charSeeker = new BufferedCharSeeker(ThreadAheadReadable.threadAhead(readable, bufferSize), bufferSize);
                    int columnDelimiter = '|';
                    Extractors extractors = new Extractors(';');
                    WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
                    lines += readingStreamPerformanceTest(writeEventStreamReader);
                    charSeeker.close();
                }
                long endTimeAsMilli = timeSource.nowAsMilli();
                long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
                lines = lines / repetitions;

                System.out.println(
                        String.format("%s took %s to read %s line: %s lines/s",
                                WriteEventStreamReaderCharSeeker.class.getSimpleName() + "-" + ThreadAheadReadable.class.getSimpleName() + "-" + bufferSize,
                                TEMPORAL_UTIL.milliDurationToString(durationAsMilli),
                                numberFormatter.format(lines),
                                numberFormatter.format((double) lines / TEMPORAL_UTIL.convert(durationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
                        )
                );
            }
        }
    }

    public long readingStreamPerformanceTest(Iterator parser) throws FileNotFoundException {
        long lines = 0;
        while (parser.hasNext()) {
            parser.next();
            lines++;
        }
        return lines;
    }

    public long doCharSeekerPerformanceTest(File forumUpdateStream, int bufferSize) throws IOException {
        CharSeeker seeker = new BufferedCharSeeker(new FileReader(forumUpdateStream), bufferSize);
        long lines = 0;
        Mark mark = new Mark();
        int[] delimiters = new int[]{'|'};
        Extractors extractors = new Extractors(';');
        while (seeker.seek(mark, delimiters)) {
            seeker.extract(mark, extractors.string()).value();
            if (mark.isEndOfLine())
                lines++;
        }
        seeker.close();
        return lines;
    }

    public long doThreadedCharSeekerPerformanceTest(File forumUpdateStream, int bufferSize) throws IOException {
        CharSeeker seeker = new BufferedCharSeeker(ThreadAheadReadable.threadAhead(new FileReader(forumUpdateStream), bufferSize), bufferSize);
        long lines = 0;
        Mark mark = new Mark();
        int[] delimiters = new int[]{'|'};
        Extractors extractors = new Extractors(';');
        while (seeker.seek(mark, delimiters)) {
            seeker.extract(mark, extractors.string()).value();
            if (mark.isEndOfLine())
                lines++;
        }
        seeker.close();
        return lines;
    }

    public long doBufferedReaderPerformanceTest(File forumUpdateStream, int bufferSize) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(forumUpdateStream), bufferSize);
        long lines = 0;
        while (null != bufferedReader.readLine()) {
            lines++;
        }
        bufferedReader.close();
        return lines;
    }
}
