package com.ldbc.driver.generator;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConservativeDependencyTimeAssigningOperationGeneratorTest
{
    @Test
    public void testAssignDependencyTimesEqualToLastEncounteredLowerDependencyStartTime()
    {
        // Given
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );
        long initialDependencyTime = 0l;

        Iterator<Operation> operations = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 1l, 1l, -1, null ), // R_W
                new TimedNamedOperation2( 2l, 2l, -1, null ), // R
                new TimedNamedOperation1( 3l, 3l, -1, null ), // R_W
                new TimedNamedOperation2( 3l, 3l, -1, null ), // R
                new TimedNamedOperation1( 5l, 5l, -1, null ), // R_W
                new TimedNamedOperation2( 6l, 6l, -1, null ), // R
                new TimedNamedOperation1( 7l, 7l, -1, null ), // R_W
                new TimedNamedOperation2( 7l, 7l, -1, null ), // R
                new TimedNamedOperation2( 8l, 8l, -1, null )   // R
        ).iterator();
        Function1<Operation,Boolean,RuntimeException> isDependency =
                new Function1<Operation,Boolean,RuntimeException>()
                {
                    @Override
                    public Boolean apply( Operation operation )
                    {
                        return operation.getClass().equals( TimedNamedOperation1.class );
                    }
                };
        boolean canOverwriteDependencyTime = true;

        // When
        Iterator<Operation> operationsWithDependencyTimes =
                gf.assignDependencyTimesEqualToLastEncounteredLowerDependencyTimeStamp( operations, isDependency,
                        initialDependencyTime, canOverwriteDependencyTime );

        // Then

        Operation operation1 = operationsWithDependencyTimes.next();
        assertThat( operation1.scheduledStartTimeAsMilli(), equalTo( 1l ) );
        assertThat( operation1.timeStamp(), equalTo( 1l ) );
        assertThat( operation1.dependencyTimeStamp(), equalTo( 0l ) );

        Operation operation2 = operationsWithDependencyTimes.next();
        assertThat( operation2.scheduledStartTimeAsMilli(), equalTo( 2l ) );
        assertThat( operation2.timeStamp(), equalTo( 2l ) );
        assertThat( operation2.dependencyTimeStamp(), equalTo( 1l ) );

        Operation operation3 = operationsWithDependencyTimes.next();
        assertThat( operation3.scheduledStartTimeAsMilli(), equalTo( 3l ) );
        assertThat( operation3.timeStamp(), equalTo( 3l ) );
        assertThat( operation3.dependencyTimeStamp(), equalTo( 1l ) );

        Operation operation4 = operationsWithDependencyTimes.next();
        assertThat( operation4.scheduledStartTimeAsMilli(), equalTo( 3l ) );
        assertThat( operation4.timeStamp(), equalTo( 3l ) );
        assertThat( operation4.dependencyTimeStamp(), equalTo( 1l ) );

        Operation operation5 = operationsWithDependencyTimes.next();
        assertThat( operation5.scheduledStartTimeAsMilli(), equalTo( 5l ) );
        assertThat( operation5.timeStamp(), equalTo( 5l ) );
        assertThat( operation5.dependencyTimeStamp(), equalTo( 3l ) );

        Operation operation6 = operationsWithDependencyTimes.next();
        assertThat( operation6.scheduledStartTimeAsMilli(), equalTo( 6l ) );
        assertThat( operation6.timeStamp(), equalTo( 6l ) );
        assertThat( operation6.dependencyTimeStamp(), equalTo( 5l ) );

        Operation operation7 = operationsWithDependencyTimes.next();
        assertThat( operation7.scheduledStartTimeAsMilli(), equalTo( 7l ) );
        assertThat( operation7.timeStamp(), equalTo( 7l ) );
        assertThat( operation7.dependencyTimeStamp(), equalTo( 5l ) );

        Operation operation8 = operationsWithDependencyTimes.next();
        assertThat( operation8.scheduledStartTimeAsMilli(), equalTo( 7l ) );
        assertThat( operation8.timeStamp(), equalTo( 7l ) );
        assertThat( operation8.dependencyTimeStamp(), equalTo( 5l ) );

        Operation operation9 = operationsWithDependencyTimes.next();
        assertThat( operation9.scheduledStartTimeAsMilli(), equalTo( 8l ) );
        assertThat( operation9.timeStamp(), equalTo( 8l ) );
        assertThat( operation9.dependencyTimeStamp(), equalTo( 7l ) );

        assertThat( operationsWithDependencyTimes.hasNext(), is( false ) );
    }

    @Test
    public void testAssignDependencyTimesEqualToLastEncounteredDependencyStartTime()
    {
        // Given
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );
        long initialDependencyTime = 0l;

        Iterator<Operation> operations = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 1l, 1l, -1, null ), // R_W
                new TimedNamedOperation2( 2l, 2l, -1, null ), // R
                new TimedNamedOperation1( 3l, 3l, -1, null ), // R_W
                new TimedNamedOperation2( 3l, 3l, -1, null ), // R
                new TimedNamedOperation1( 5l, 5l, -1, null ), // R_W
                new TimedNamedOperation2( 6l, 6l, -1, null ), // R
                new TimedNamedOperation1( 7l, 7l, -1, null ), // R_W
                new TimedNamedOperation2( 7l, 7l, -1, null ), // R
                new TimedNamedOperation2( 8l, 8l, -1, null )   // R
        ).iterator();
        Function1<Operation,Boolean,RuntimeException> isDependency =
                new Function1<Operation,Boolean,RuntimeException>()
                {
                    @Override
                    public Boolean apply( Operation operation )
                    {
                        return operation.getClass().equals( TimedNamedOperation1.class );
                    }
                };
        boolean canOverwriteDependencyTime = true;

        // When
        Iterator<Operation> operationsWithDependencyTimes =
                gf.assignDependencyTimesEqualToLastEncounteredDependencyTimeStamp( operations, isDependency,
                        initialDependencyTime, canOverwriteDependencyTime );

        // Then

        Operation operation1 = operationsWithDependencyTimes.next();
        assertThat( operation1.scheduledStartTimeAsMilli(), equalTo( 1l ) );
        assertThat( operation1.timeStamp(), equalTo( 1l ) );
        assertThat( operation1.dependencyTimeStamp(), equalTo( 0l ) );

        Operation operation2 = operationsWithDependencyTimes.next();
        assertThat( operation2.scheduledStartTimeAsMilli(), equalTo( 2l ) );
        assertThat( operation2.timeStamp(), equalTo( 2l ) );
        assertThat( operation2.dependencyTimeStamp(), equalTo( 1l ) );

        Operation operation3 = operationsWithDependencyTimes.next();
        assertThat( operation3.scheduledStartTimeAsMilli(), equalTo( 3l ) );
        assertThat( operation3.timeStamp(), equalTo( 3l ) );
        assertThat( operation3.dependencyTimeStamp(), equalTo( 1l ) );

        Operation operation4 = operationsWithDependencyTimes.next();
        assertThat( operation4.scheduledStartTimeAsMilli(), equalTo( 3l ) );
        assertThat( operation4.timeStamp(), equalTo( 3l ) );
        assertThat( operation4.dependencyTimeStamp(), equalTo( 3l ) );

        Operation operation5 = operationsWithDependencyTimes.next();
        assertThat( operation5.scheduledStartTimeAsMilli(), equalTo( 5l ) );
        assertThat( operation5.timeStamp(), equalTo( 5l ) );
        assertThat( operation5.dependencyTimeStamp(), equalTo( 3l ) );

        Operation operation6 = operationsWithDependencyTimes.next();
        assertThat( operation6.scheduledStartTimeAsMilli(), equalTo( 6l ) );
        assertThat( operation6.timeStamp(), equalTo( 6l ) );
        assertThat( operation6.dependencyTimeStamp(), equalTo( 5l ) );

        Operation operation7 = operationsWithDependencyTimes.next();
        assertThat( operation7.scheduledStartTimeAsMilli(), equalTo( 7l ) );
        assertThat( operation7.timeStamp(), equalTo( 7l ) );
        assertThat( operation7.dependencyTimeStamp(), equalTo( 5l ) );

        Operation operation8 = operationsWithDependencyTimes.next();
        assertThat( operation8.scheduledStartTimeAsMilli(), equalTo( 7l ) );
        assertThat( operation8.timeStamp(), equalTo( 7l ) );
        assertThat( operation8.dependencyTimeStamp(), equalTo( 7l ) );

        Operation operation9 = operationsWithDependencyTimes.next();
        assertThat( operation9.scheduledStartTimeAsMilli(), equalTo( 8l ) );
        assertThat( operation9.timeStamp(), equalTo( 8l ) );
        assertThat( operation9.dependencyTimeStamp(), equalTo( 7l ) );

        assertThat( operationsWithDependencyTimes.hasNext(), is( false ) );
    }

    @Test
    public void testAssignConservativeDependencyTimes()
    {
        // Given
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );
        long initialDependencyTime = 0l;

        Iterator<Operation> operations = Lists.<Operation>newArrayList(
                new TimedNamedOperation1( 1l, 1l, -1, null ), // R_W
                new TimedNamedOperation2( 2l, 2l, -1, null ), // R
                new TimedNamedOperation1( 3l, 3l, -1, null ), // R_W
                new TimedNamedOperation2( 3l, 3l, -1, null ), // R
                new TimedNamedOperation1( 5l, 5l, -1, null ), // R_W
                new TimedNamedOperation2( 6l, 6l, -1, null ), // R
                new TimedNamedOperation1( 7l, 7l, -1, null ), // R_W
                new TimedNamedOperation2( 7l, 7l, -1, null ), // R
                new TimedNamedOperation2( 8l, 8l, -1, null )   // R
        ).iterator();

        boolean canOverwriteDependencyTime = true;

        // When
        Iterator<Operation> operationsWithDependencyTimes =
                gf.assignConservativeDependencyTimes( operations, initialDependencyTime, canOverwriteDependencyTime );

        // Then

        Operation operation1 = operationsWithDependencyTimes.next();
        assertThat( operation1.scheduledStartTimeAsMilli(), equalTo( 1l ) );
        assertThat( operation1.timeStamp(), equalTo( 1l ) );
        assertThat( operation1.dependencyTimeStamp(), equalTo( 0l ) );

        Operation operation2 = operationsWithDependencyTimes.next();
        assertThat( operation2.scheduledStartTimeAsMilli(), equalTo( 2l ) );
        assertThat( operation2.timeStamp(), equalTo( 2l ) );
        assertThat( operation2.dependencyTimeStamp(), equalTo( 1l ) );

        Operation operation3 = operationsWithDependencyTimes.next();
        assertThat( operation3.scheduledStartTimeAsMilli(), equalTo( 3l ) );
        assertThat( operation3.timeStamp(), equalTo( 3l ) );
        assertThat( operation3.dependencyTimeStamp(), equalTo( 2l ) );

        Operation operation4 = operationsWithDependencyTimes.next();
        assertThat( operation4.scheduledStartTimeAsMilli(), equalTo( 3l ) );
        assertThat( operation4.timeStamp(), equalTo( 3l ) );
        assertThat( operation4.dependencyTimeStamp(), equalTo( 3l ) );

        Operation operation5 = operationsWithDependencyTimes.next();
        assertThat( operation5.scheduledStartTimeAsMilli(), equalTo( 5l ) );
        assertThat( operation5.timeStamp(), equalTo( 5l ) );
        assertThat( operation5.dependencyTimeStamp(), equalTo( 3l ) );

        Operation operation6 = operationsWithDependencyTimes.next();
        assertThat( operation6.scheduledStartTimeAsMilli(), equalTo( 6l ) );
        assertThat( operation6.timeStamp(), equalTo( 6l ) );
        assertThat( operation6.dependencyTimeStamp(), equalTo( 5l ) );

        Operation operation7 = operationsWithDependencyTimes.next();
        assertThat( operation7.scheduledStartTimeAsMilli(), equalTo( 7l ) );
        assertThat( operation7.timeStamp(), equalTo( 7l ) );
        assertThat( operation7.dependencyTimeStamp(), equalTo( 6l ) );

        Operation operation8 = operationsWithDependencyTimes.next();
        assertThat( operation8.scheduledStartTimeAsMilli(), equalTo( 7l ) );
        assertThat( operation8.timeStamp(), equalTo( 7l ) );
        assertThat( operation8.dependencyTimeStamp(), equalTo( 7l ) );

        Operation operation9 = operationsWithDependencyTimes.next();
        assertThat( operation9.scheduledStartTimeAsMilli(), equalTo( 8l ) );
        assertThat( operation9.timeStamp(), equalTo( 8l ) );
        assertThat( operation9.dependencyTimeStamp(), equalTo( 7l ) );

        assertThat( operationsWithDependencyTimes.hasNext(), is( false ) );
    }
}
