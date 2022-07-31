package org.ldbcouncil.snb.driver.generator;

import com.google.common.collect.Lists;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.util.Function1;
import org.ldbcouncil.snb.driver.workloads.dummy.TimedNamedOperation1;
import org.ldbcouncil.snb.driver.workloads.dummy.TimedNamedOperation2;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

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
        assertEquals( 1l,  operation1.scheduledStartTimeAsMilli() );
        assertEquals( 1l, operation1.timeStamp());
        assertEquals( 0l, operation1.dependencyTimeStamp());

        Operation operation2 = operationsWithDependencyTimes.next();
        assertEquals( 2l, operation2.scheduledStartTimeAsMilli() );
        assertEquals( 2l, operation2.timeStamp() );
        assertEquals( 1l, operation2.dependencyTimeStamp() );

        Operation operation3 = operationsWithDependencyTimes.next();
        assertEquals( 3l, operation3.scheduledStartTimeAsMilli());
        assertEquals( 3l, operation3.timeStamp() );
        assertEquals( 1l, operation3.dependencyTimeStamp());

        Operation operation4 = operationsWithDependencyTimes.next();
        assertEquals( 3l, operation4.scheduledStartTimeAsMilli() );
        assertEquals( 3l, operation4.timeStamp() );
        assertEquals( 1l, operation4.dependencyTimeStamp() );

        Operation operation5 = operationsWithDependencyTimes.next();
        assertEquals( 5l, operation5.scheduledStartTimeAsMilli() );
        assertEquals( 5l, operation5.timeStamp() );
        assertEquals( 3l, operation5.dependencyTimeStamp());

        Operation operation6 = operationsWithDependencyTimes.next();
        assertEquals( 6l, operation6.scheduledStartTimeAsMilli() );
        assertEquals( 6l, operation6.timeStamp() );
        assertEquals( 5l, operation6.dependencyTimeStamp() );

        Operation operation7 = operationsWithDependencyTimes.next();
        assertEquals( 7l, operation7.scheduledStartTimeAsMilli());
        assertEquals( 7l, operation7.timeStamp() );
        assertEquals( 5l, operation7.dependencyTimeStamp() );

        Operation operation8 = operationsWithDependencyTimes.next();
        assertEquals( 7l, operation8.scheduledStartTimeAsMilli());
        assertEquals( 7l, operation8.timeStamp() );
        assertEquals( 5l, operation8.dependencyTimeStamp() );

        Operation operation9 = operationsWithDependencyTimes.next();
        assertEquals( 8l, operation9.scheduledStartTimeAsMilli() );
        assertEquals( 8l, operation9.timeStamp() );
        assertEquals( 7l, operation9.dependencyTimeStamp() );

        assertFalse( operationsWithDependencyTimes.hasNext() );
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
        assertEquals( 1l,  operation1.scheduledStartTimeAsMilli() );
        assertEquals( 1l, operation1.timeStamp());
        assertEquals( 0l, operation1.dependencyTimeStamp());

        Operation operation2 = operationsWithDependencyTimes.next();
        assertEquals( 2l, operation2.scheduledStartTimeAsMilli() );
        assertEquals( 2l, operation2.timeStamp() );
        assertEquals( 1l, operation2.dependencyTimeStamp() );

        Operation operation3 = operationsWithDependencyTimes.next();
        assertEquals( 3l, operation3.scheduledStartTimeAsMilli());
        assertEquals( 3l, operation3.timeStamp() );
        assertEquals( 1l, operation3.dependencyTimeStamp());

        Operation operation4 = operationsWithDependencyTimes.next();
        assertEquals( 3l, operation4.scheduledStartTimeAsMilli() );
        assertEquals( 3l, operation4.timeStamp() );
        assertEquals( 1l, operation4.dependencyTimeStamp() );

        Operation operation5 = operationsWithDependencyTimes.next();
        assertEquals( 5l, operation5.scheduledStartTimeAsMilli() );
        assertEquals( 5l, operation5.timeStamp() );
        assertEquals( 3l, operation5.dependencyTimeStamp());

        Operation operation6 = operationsWithDependencyTimes.next();
        assertEquals( 6l, operation6.scheduledStartTimeAsMilli() );
        assertEquals( 6l, operation6.timeStamp() );
        assertEquals( 5l, operation6.dependencyTimeStamp() );

        Operation operation7 = operationsWithDependencyTimes.next();
        assertEquals( 7l, operation7.scheduledStartTimeAsMilli());
        assertEquals( 7l, operation7.timeStamp() );
        assertEquals( 5l, operation7.dependencyTimeStamp() );

        Operation operation8 = operationsWithDependencyTimes.next();
        assertEquals( 7l, operation8.scheduledStartTimeAsMilli());
        assertEquals( 7l, operation8.timeStamp() );
        assertEquals( 5l, operation8.dependencyTimeStamp() );

        Operation operation9 = operationsWithDependencyTimes.next();
        assertEquals( 8l, operation9.scheduledStartTimeAsMilli() );
        assertEquals( 8l, operation9.timeStamp() );
        assertEquals( 7l, operation9.dependencyTimeStamp() );

        assertFalse( operationsWithDependencyTimes.hasNext() );
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
        assertEquals( 1l,  operation1.scheduledStartTimeAsMilli() );
        assertEquals( 1l, operation1.timeStamp());
        assertEquals( 0l, operation1.dependencyTimeStamp());

        Operation operation2 = operationsWithDependencyTimes.next();
        assertEquals( 2l, operation2.scheduledStartTimeAsMilli() );
        assertEquals( 2l, operation2.timeStamp() );
        assertEquals( 1l, operation2.dependencyTimeStamp() );

        Operation operation3 = operationsWithDependencyTimes.next();
        assertEquals( 3l, operation3.scheduledStartTimeAsMilli());
        assertEquals( 3l, operation3.timeStamp() );
        assertEquals( 2l, operation3.dependencyTimeStamp());

        Operation operation4 = operationsWithDependencyTimes.next();
        assertEquals( 3l, operation4.scheduledStartTimeAsMilli() );
        assertEquals( 3l, operation4.timeStamp() );
        assertEquals( 3l, operation4.dependencyTimeStamp() );

        Operation operation5 = operationsWithDependencyTimes.next();
        assertEquals( 5l, operation5.scheduledStartTimeAsMilli() );
        assertEquals( 5l, operation5.timeStamp() );
        assertEquals( 3l, operation5.dependencyTimeStamp());

        Operation operation6 = operationsWithDependencyTimes.next();
        assertEquals( 6l, operation6.scheduledStartTimeAsMilli() );
        assertEquals( 6l, operation6.timeStamp() );
        assertEquals( 5l, operation6.dependencyTimeStamp() );

        Operation operation7 = operationsWithDependencyTimes.next();
        assertEquals( 7l, operation7.scheduledStartTimeAsMilli());
        assertEquals( 7l, operation7.timeStamp() );
        assertEquals( 6l, operation7.dependencyTimeStamp() );

        Operation operation8 = operationsWithDependencyTimes.next();
        assertEquals( 7l, operation8.scheduledStartTimeAsMilli());
        assertEquals( 7l, operation8.timeStamp() );
        assertEquals( 7l, operation8.dependencyTimeStamp() );

        Operation operation9 = operationsWithDependencyTimes.next();
        assertEquals( 8l, operation9.scheduledStartTimeAsMilli() );
        assertEquals( 8l, operation9.timeStamp() );
        assertEquals( 7l, operation9.dependencyTimeStamp() );

        assertFalse( operationsWithDependencyTimes.hasNext() );
    }
}
