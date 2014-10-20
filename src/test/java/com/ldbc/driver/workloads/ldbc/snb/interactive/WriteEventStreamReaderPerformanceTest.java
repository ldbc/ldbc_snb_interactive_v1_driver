package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.csv.*;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.util.Iterator;
import java.util.List;

public class WriteEventStreamReaderPerformanceTest {
    TimeSource timeSource = new SystemTimeSource();

    @Ignore
    @Test
    public void newFormatLongDateCsvUpdateStreamReadingRegexParserProfileTest() throws IOException, InterruptedException {
        Thread.sleep(30000);
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/format_csv_date_long/");
        File forumUpdateStream = new File(parentStreamsDir, "sf_10_partitions_01/updateStream_0_0_forum.csv");

        long lines = 0;
        long startTimeAsMilli = timeSource.nowAsMilli();
        SimpleCsvFileReader simpleCsvFileReader = new SimpleCsvFileReader(forumUpdateStream, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(simpleCsvFileReader);
        lines += doCsvUpdateStreamReadingPerformanceTest(writeEventStreamReader);
        simpleCsvFileReader.close();
        long endTimeAsMilli = timeSource.nowAsMilli();
        long durationAsMilli = (endTimeAsMilli - startTimeAsMilli);

        System.out.println(
                String.format("%s took %s to read %s line: %s lines/s",
                        WriteEventStreamReaderRegex.class.getSimpleName(),
                        Duration.fromMilli(durationAsMilli),
                        lines,
                        (double) lines / Duration.fromMilli(durationAsMilli).asSeconds()
                )
        );
    }

    @Ignore
    @Test
    public void newFormatLongDateCsvUpdateStreamReadingCharSeekerParserProfileTest() throws IOException, InterruptedException {
        Thread.sleep(30000);
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/format_csv_date_long/");
        File forumUpdateStream = new File(parentStreamsDir, "sf_10_partitions_01/updateStream_0_0_forum.csv");

        int MB = 1024 * 1024;
        int bufferSize = 2 * MB;
        long lines = 0;
        long startTimeAsMilli = timeSource.nowAsMilli();
        CharSeeker charSeeker = new BufferedCharSeeker(new InputStreamReader(new FileInputStream(forumUpdateStream), Charsets.UTF_8), bufferSize);
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
        lines += doCsvUpdateStreamReadingPerformanceTest(writeEventStreamReader);
        charSeeker.close();
        long endTimeAsMilli = timeSource.nowAsMilli();
        long durationAsMilli = (endTimeAsMilli - startTimeAsMilli);

        System.out.println(
                String.format("%s took %s to read %s line: %s lines/s",
                        WriteEventStreamReaderCharSeeker.class.getSimpleName() + "-" + bufferSize,
                        Duration.fromMilli(durationAsMilli),
                        lines,
                        (double) lines / Duration.fromMilli(durationAsMilli).asSeconds()
                )
        );
    }

    @Ignore
    @Test
    public void newFormatLongDateCsvUpdateStreamReadingPerformanceTest() throws IOException {
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/scale_factor_streams/format_csv_date_long/");
        File forumUpdateStream = new File(parentStreamsDir, "sf_10_partitions_01/updateStream_0_0_forum.csv");

        {
            // warm up file system
            SimpleCsvFileReader simpleCsvFileReader = new SimpleCsvFileReader(forumUpdateStream, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
            doCsvUpdateStreamReadingPerformanceTest(simpleCsvFileReader);
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
                            Duration.fromMilli(durationAsMilli),
                            lines,
                            (double) lines / Duration.fromMilli(durationAsMilli).asSeconds()
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
                            Duration.fromMilli(durationAsMilli),
                            lines,
                            (double) lines / Duration.fromMilli(durationAsMilli).asSeconds()
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
                            Duration.fromMilli(durationAsMilli),
                            lines,
                            (double) lines / Duration.fromMilli(durationAsMilli).asSeconds()
                    )
            );
        }

        {
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {
                SimpleCsvFileReader simpleCsvFileReader = new SimpleCsvFileReader(forumUpdateStream, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
                lines += doCsvUpdateStreamReadingPerformanceTest(simpleCsvFileReader);
                simpleCsvFileReader.close();
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            SimpleCsvFileReader.class.getSimpleName(),
                            Duration.fromMilli(durationAsMilli),
                            lines,
                            (double) lines / Duration.fromMilli(durationAsMilli).asSeconds()
                    )
            );
        }

        {
            long lines = 0;
            long startTimeAsMilli = timeSource.nowAsMilli();
            for (int i = 0; i < repetitions; i++) {
                SimpleCsvFileReader simpleCsvFileReader = new SimpleCsvFileReader(forumUpdateStream, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
                WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(simpleCsvFileReader);
                lines += doCsvUpdateStreamReadingPerformanceTest(writeEventStreamReader);
                simpleCsvFileReader.close();
            }
            long endTimeAsMilli = timeSource.nowAsMilli();
            long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
            lines = lines / repetitions;

            System.out.println(
                    String.format("%s took %s to read %s line: %s lines/s",
                            WriteEventStreamReaderRegex.class.getSimpleName(),
                            Duration.fromMilli(durationAsMilli),
                            lines,
                            (double) lines / Duration.fromMilli(durationAsMilli).asSeconds()
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
                    lines += doCsvUpdateStreamReadingPerformanceTest(writeEventStreamReader);
                    charSeeker.close();
                }
                long endTimeAsMilli = timeSource.nowAsMilli();
                long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
                lines = lines / repetitions;

                System.out.println(
                        String.format("%s took %s to read %s line: %s lines/s",
                                WriteEventStreamReaderCharSeeker.class.getSimpleName() + "-" + bufferSize,
                                Duration.fromMilli(durationAsMilli),
                                lines,
                                (double) lines / Duration.fromMilli(durationAsMilli).asSeconds()
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
                    lines += doCsvUpdateStreamReadingPerformanceTest(writeEventStreamReader);
                    charSeeker.close();
                }
                long endTimeAsMilli = timeSource.nowAsMilli();
                long durationAsMilli = (endTimeAsMilli - startTimeAsMilli) / repetitions;
                lines = lines / repetitions;

                System.out.println(
                        String.format("%s took %s to read %s line: %s lines/s",
                                WriteEventStreamReaderCharSeeker.class.getSimpleName() + "-" + ThreadAheadReadable.class.getSimpleName() + "-" + bufferSize,
                                Duration.fromMilli(durationAsMilli),
                                lines,
                                (double) lines / Duration.fromMilli(durationAsMilli).asSeconds()
                        )
                );
            }
        }
    }

    public long doCsvUpdateStreamReadingPerformanceTest(Iterator fileParser) throws FileNotFoundException {
        long lines = 0;
        while (fileParser.hasNext()) {
            fileParser.next();
            lines++;
        }
        return lines;
    }

    public long doCharSeekerPerformanceTest(File forumUpdateStream, int bufferSize) throws IOException {
        CharSeeker seeker = new BufferedCharSeeker(new FileReader(forumUpdateStream), bufferSize);
        long lines = 0;
        Mark mark = new Mark();
        int[] delimiters = new int[]{'|'};
        boolean tryAgain = true;
        while (tryAgain) {
            String value = null;
            while (seeker.seek(mark, delimiters)) {
                value = seeker.extract(mark, Extractors.STRING);
            }
            lines++;
            tryAgain = value != null;
        }
        seeker.close();
        return lines;
    }

    public long doThreadedCharSeekerPerformanceTest(File forumUpdateStream, int bufferSize) throws IOException {
        CharSeeker seeker = new BufferedCharSeeker(ThreadAheadReadable.threadAhead(new FileReader(forumUpdateStream), bufferSize), bufferSize);
        long lines = 0;
        Mark mark = new Mark();
        int[] delimiters = new int[]{'|'};
        boolean tryAgain = true;
        while (tryAgain) {
            String value = null;
            while (seeker.seek(mark, delimiters)) {
                value = seeker.extract(mark, Extractors.STRING);
            }
            lines++;
            tryAgain = value != null;
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
