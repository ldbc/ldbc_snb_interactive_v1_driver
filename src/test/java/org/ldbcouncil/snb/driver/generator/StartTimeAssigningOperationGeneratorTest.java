package org.ldbcouncil.snb.driver.generator;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.util.Function1;
import org.ldbcouncil.snb.driver.workloads.dummy.NothingOperationFactory;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
            assertEquals( lastTime + incrementMilliTimeBy, operation.scheduledStartTimeAsMilli());
            assertEquals( lastTime + incrementMilliTimeBy, operation.timeStamp() );
            lastTime = operation.scheduledStartTimeAsMilli();
            count++;
        }
        assertEquals(testIterations, count);
    }
}
