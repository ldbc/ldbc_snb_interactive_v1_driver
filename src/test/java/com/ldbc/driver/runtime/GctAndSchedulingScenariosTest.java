package com.ldbc.driver.runtime;

import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GctAndSchedulingScenariosTest {
    @Test
    public void test() throws DriverConfigurationException {
        String databaseClassName = null;
        String workloadClassName = null;
        long operationCount = 0;
        ConsoleAndFileDriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults(databaseClassName, workloadClassName, operationCount);



        assertThat(true, is(false));
    }
}
