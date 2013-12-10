package com.ldbc.driver.generator;

import java.util.Iterator;

import org.junit.Test;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.IdentityGenerator;
import com.ldbc.driver.generator.MappingGenerator;
import com.ldbc.driver.generator.OperationStartTimeAdvancingGenerator;
import com.ldbc.driver.generator.StartTimeAssigningOperationGenerator;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.temporal.Time;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

public class OperationStartTimeAdvancingGeneratorTest
{
    @Test
    public void shouldShiftTimeForward()
    {
        // Given
        Iterator<Long> timeInNanoGenerator = new IdentityGenerator<Long>( 10l, 20l, 30l, 40l, 50l );

        Function1<Long, Time> timeFromNanoFun = new Function1<Long, Time>()
        {
            @Override
            public Time apply( Long from )
            {
                return Time.fromNano( from );
            }
        };
        Iterator<Time> timeGenerator = new MappingGenerator<Long, Time>( timeInNanoGenerator, timeFromNanoFun );

        Iterator<Operation<?>> operationGenerator = new StartTimeAssigningOperationGenerator( timeGenerator,
                new OperationGenerator() );

        // When
        Iterator<Operation<?>> shiftedOperationGenerator = new OperationStartTimeAdvancingGenerator( operationGenerator,
                Time.fromNano( 60l ) );

        // Then
        assertThat( shiftedOperationGenerator.next().scheduledStartTime(), is( Time.fromNano( 60l ) ) );
        assertThat( shiftedOperationGenerator.next().scheduledStartTime(), is( Time.fromNano( 70l ) ) );
        assertThat( shiftedOperationGenerator.next().scheduledStartTime(), is( Time.fromNano( 80l ) ) );
        assertThat( shiftedOperationGenerator.next().scheduledStartTime(), is( Time.fromNano( 90l ) ) );
        assertThat( shiftedOperationGenerator.next().scheduledStartTime(), is( Time.fromNano( 100l ) ) );
        assertThat( shiftedOperationGenerator.hasNext(), is( false ) );
    }

    static class OperationGenerator extends Generator<Operation<?>>
    {
        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new Operation<Object>()
            {
            };
        }
    }
}
