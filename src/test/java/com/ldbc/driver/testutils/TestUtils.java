package com.ldbc.driver.testutils;

import com.ldbc.driver.temporal.TimeSource;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Iterator;

import static java.lang.String.format;

public class TestUtils
{
    public static File getResource( String path )
    {
        return FileUtils.toFile( TestUtils.class.getResource( path ) );
    }

    public static ThreadPoolLoadGenerator newThreadPoolLoadGenerator( int threadCount, long sleepDurationAsMilli )
    {
        return new ThreadPoolLoadGenerator( threadCount, sleepDurationAsMilli );
    }

    public static <T> boolean generateBeforeTimeout(
            Iterator<T> generator,
            long timeoutAsMilli,
            TimeSource timeSource,
            long itemsToGenerate )
    {
        long startTimeAsMilli = timeSource.nowAsMilli();
        long itemsGenerated = 0;
        while ( generator.hasNext() )
        {
            generator.next();
            itemsGenerated++;
            // check if generated enough items yet
            if ( itemsGenerated >= itemsToGenerate )
            {
                break;
            }
            // occasionally check for timeout
            if ( (itemsGenerated % 1000 == 0) && (timeSource.nowAsMilli() > timeoutAsMilli) )
            {
                break;
            }
        }
        long finishTimeAsMilli = timeSource.nowAsMilli();
        boolean result = (finishTimeAsMilli < timeoutAsMilli) &&
                         (itemsGenerated >= itemsToGenerate || false == generator.hasNext());
        long testDurationAsMilli = finishTimeAsMilli - startTimeAsMilli;
        System.out.println(
                format( "Generated %s elements in %s ms ---> %s elements/ms",
                        itemsGenerated,
                        testDurationAsMilli,
                        (double) itemsGenerated /
                        testDurationAsMilli ) );
        return result;
    }
}
