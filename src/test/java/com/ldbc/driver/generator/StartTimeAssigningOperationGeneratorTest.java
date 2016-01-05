package com.ldbc.driver.generator;

import com.ldbc.driver.Operation;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.workloads.dummy.NothingOperationFactory;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class StartTimeAssigningOperationGeneratorTest
{
    @Test
    public void shouldAssignStartTimesToOperations()
    {
        // Given
        long firstMilliTime = 1000;
        long incrementMilliTimeBy = 100;
        int testIterations = 10;
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );

        // When
        Iterator<Operation> operations = gf.limit( new NothingOperationFactory(), testIterations );
        Function1<Long,Long,RuntimeException> timeFromLongFun = new Function1<Long,Long,RuntimeException>()
        {
            @Override
            public Long apply( Long from )
            {
                return from;
            }
        };

        Iterator<Long> countGenerator = gf.incrementing( firstMilliTime, incrementMilliTimeBy );
        Iterator<Long> counterStartTimeGenerator = gf.map( countGenerator, timeFromLongFun );
        Iterator<Operation> startTimeOperationGenerator = gf.assignStartTimes( counterStartTimeGenerator, operations );

        // Then
        int count = 0;
        long lastTime = firstMilliTime - incrementMilliTimeBy;
        while ( startTimeOperationGenerator.hasNext() )
        {
            Operation operation = startTimeOperationGenerator.next();
            assertThat( operation.scheduledStartTimeAsMilli(), is( lastTime + incrementMilliTimeBy ) );
            assertThat( operation.timeStamp(), is( lastTime + incrementMilliTimeBy ) );
            lastTime = operation.scheduledStartTimeAsMilli();
            count++;
        }
        assertThat( count, is( testIterations ) );
    }
}
