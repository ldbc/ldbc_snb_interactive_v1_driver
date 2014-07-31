package com.ldbc.driver.testutils;

import com.ldbc.driver.temporal.Duration;
import org.apache.commons.io.FileUtils;

import java.io.File;

public class TestUtils {
    public static File getResource(String path) {
        return FileUtils.toFile(TestUtils.class.getResource(path));
    }

    public static ThreadPoolLoadGenerator newThreadPoolLoadGenerator(int threadCount, Duration sleepDuration) {
        return new ThreadPoolLoadGenerator(threadCount, sleepDuration);
    }
}
