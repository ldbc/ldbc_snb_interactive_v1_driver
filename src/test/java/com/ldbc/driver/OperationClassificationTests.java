package com.ldbc.driver;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class OperationClassificationTests {
    @Test
    public void shouldPerformEqualityCorrectly() {
        // Given
        OperationClassification classification1 = new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE);
        OperationClassification classification2 = new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.NONE);
        OperationClassification classification3 = new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.GctMode.READ);
        OperationClassification classification4 = new OperationClassification(OperationClassification.SchedulingMode.WINDOWED, OperationClassification.GctMode.NONE);

        // When

        // Then
        assertThat(classification1, equalTo(classification1));
        assertThat(classification1, equalTo(classification2));
        assertThat(classification1, not(equalTo(classification3)));
        assertThat(classification1, not(equalTo(classification4)));

        assertThat(classification2, equalTo(classification1));
        assertThat(classification2, equalTo(classification2));
        assertThat(classification2, not(equalTo(classification3)));
        assertThat(classification2, not(equalTo(classification4)));

        assertThat(classification3, not(equalTo(classification1)));
        assertThat(classification3, not(equalTo(classification2)));
        assertThat(classification3, equalTo(classification3));
        assertThat(classification3, not(equalTo(classification4)));

        assertThat(classification4, not(equalTo(classification1)));
        assertThat(classification4, not(equalTo(classification2)));
        assertThat(classification4, not(equalTo(classification3)));
        assertThat(classification4, equalTo(classification4));
    }
}
