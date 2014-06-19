package com.ldbc.driver.validation;

import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WorkloadValidatorTests {
    private GeneratorFactory generators;

    @Before
    public void init() {
        generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
    }

    @Ignore
    @Test
    public void shouldFailValidationWhenWorkloadDefinitionIsIncorrect() throws MetricsCollectionException {
        assertThat(true, is(false));
        assertThat(true, equalTo(false));
    }
}
