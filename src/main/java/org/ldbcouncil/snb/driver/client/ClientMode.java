package org.ldbcouncil.snb.driver.client;

import org.ldbcouncil.snb.driver.ClientException;

public interface ClientMode<T>
{
    void init() throws ClientException;

    T startExecutionAndAwaitCompletion() throws ClientException;
}
