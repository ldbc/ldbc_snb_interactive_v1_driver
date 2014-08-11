package com.ldbc.driver.testutils;

import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Iterator;

public class TestUtils {
    public static File getResource(String path) {
        return FileUtils.toFile(TestUtils.class.getResource(path));
    }

    public static ThreadPoolLoadGenerator newThreadPoolLoadGenerator(int threadCount, Duration sleepDuration) {
        return new ThreadPoolLoadGenerator(threadCount, sleepDuration);
    }

    public static <T> boolean generateBeforeTimeout(Iterator<T> generator, Time timeout, TimeSource timeSource, long itemsToGenerate) {
        long startTimeAsMilli = timeSource.nowAsMilli();
        long timeoutAsMilli = timeout.asMilli();
        long itemsGenerated = 0;
        while (generator.hasNext()) {
            generator.next();
            itemsGenerated++;
            // check if generated enough items yet
            if (itemsGenerated >= itemsToGenerate) break;
            // occasionally check for timeout
            if ((itemsGenerated % 1000 == 0) && (timeSource.nowAsMilli() > timeoutAsMilli)) break;
        }
        long finishTimeAsMilli = timeSource.nowAsMilli();
        boolean result = (finishTimeAsMilli < timeoutAsMilli) && (itemsGenerated >= itemsToGenerate);
        Duration testDuration = Time.fromMilli(finishTimeAsMilli).durationGreaterThan(Time.fromMilli(startTimeAsMilli));
        System.out.println(String.format("Generated %s elements in %s = %s elements/ms", itemsGenerated, testDuration, (double) itemsGenerated / testDuration.asMilli()));
        return result;
    }
}
