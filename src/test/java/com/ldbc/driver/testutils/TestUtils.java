package com.ldbc.driver.testutils;

import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.TimeSource;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

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

    public static <O extends Operation> void assertCorrectParameterMap(O operation) {
        Map<String, Object> params = operation.parameterMap();

        Set<Field> commonFields = Sets.newHashSet(Operation.class.getDeclaredFields());
        Set<Field> parameter = Stream.of(operation.getClass().getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !commonFields.contains(field))
                .collect(Collectors.toSet());

        parameter.forEach(field -> {
            Object expected = null;
            try {
                expected = operation.getClass().getDeclaredMethod(field.getName()).invoke(operation);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                e.printStackTrace();
            }
            Object actual = params.get(field.getName());

            assertThat(
                "Exptected parameter value " + field.getName() + " to be " + expected + ", but was " + actual,
                actual, equalTo(expected)
            );
        });
    }
}
