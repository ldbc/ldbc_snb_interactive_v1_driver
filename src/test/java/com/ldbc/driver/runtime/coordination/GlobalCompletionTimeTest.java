package com.ldbc.driver.runtime.coordination;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GlobalCompletionTimeTest
{
    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTV1() throws CompletionTimeException
    {
        // Given
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager );

        // When/Then
        // initiated [1]
        // completed []
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 1000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( -1L ) );

        // initiated [1]
        // completed [1]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 1000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( -1L ) );

        // initiated [1,2]
        // completed [1]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 2000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2]
        // completed [1,2]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 2000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3]
        // completed [1,2]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 3000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4]
        // completed [1,2]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 4000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 5000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, , ,5]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 5000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , , , ,  ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 6000L );
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 7000L );
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 8000L );
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 9000L );
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 10000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, , ,8, ,  ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 8000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8, ,  ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 7000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, , ,5, ,7,8,9,  ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 9000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2, ,4,5, ,7,8,9,  ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 4000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5, ,7,8,9,  ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 3000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 5000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,  ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 6000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 9000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10]
        // completed [1,2,3,4,5,6,7,8,9,10]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 10000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 9000L ) );

        // initiated [1,2,3,4,5,6,7,8,9,10,11]
        // completed [1,2,3,4,5,6,7,8,9,10,  ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 11000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 10000L ) );
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTV2() throws CompletionTimeException
    {
        // Given
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager );

        // When/Then
        // initiated [1]
        // completed [1]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 1000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( -1L ) );
        globalCompletionTimeStateManager.submitLocalCompletedTime( 1000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( -1L ) );

        // initiated [1,2]
        // completed [1,2]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 2000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 1000L ) );
        globalCompletionTimeStateManager.submitLocalCompletedTime( 2000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3]
        // completed [1,2, ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 3000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3]
        // completed [1,2, , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 3000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3]
        // completed [1,2, , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 3000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4]
        // completed [1,2, , , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 4000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5]
        // completed [1,2, , , , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 5000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , , , ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 6000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , , , ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 5000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, , ,3, ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 3000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2, ,3,3, ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 3000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3, ,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 3000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 3000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 4000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 5000L ) );

        // initiated [1,2,3,3,3,4,5,6]
        // completed [1,2,3,3,3,4,5,6]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 6000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 5000L ) );
    }

    @Test
    public void shouldReturnTimeOfEarliestITThatHasHadNoMatchingCTV3() throws CompletionTimeException
    {
        // Given
        LocalCompletionTimeStateManager localCompletionTimeStateManager = new LocalCompletionTimeStateManager();
        GlobalCompletionTimeStateManager globalCompletionTimeStateManager = new GlobalCompletionTimeStateManager(
                localCompletionTimeStateManager,
                localCompletionTimeStateManager );

        // When/Then
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( -1L ) );

        // initiated [1]
        // completed [1]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 1000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( -1L ) );

        globalCompletionTimeStateManager.submitLocalCompletedTime( 1000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( -1L ) );

        // initiated [1]
        // completed [1]
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( -1L ) );

        // initiated [1]
        // completed [1]
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( -1L ) );

        // initiated [1,2]
        // completed [1]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 2000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3]
        // completed [1]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 3000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 4000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1, , ,4]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 4000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 1000L ) );

        // initiated [1,2,3,4]
        // completed [1,2, ,4]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 2000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 5000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2, ,4, ]
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 2000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4, ]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 3000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 4000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        globalCompletionTimeStateManager.submitLocalCompletedTime( 5000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 4000L ) );

        // initiated [1,2,3,4,5]
        // completed [1,2,3,4,5]
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 4000L ) );

        // initiated [1,2,3,4,5,6]
        // completed [1,2,3,4,5]
        globalCompletionTimeStateManager.submitLocalInitiatedTime( 6000L );
        assertThat( globalCompletionTimeStateManager.globalCompletionTimeAsMilli(), is( 5000L ) );
    }
}
