package org.ldbcouncil.driver.client;

import org.ldbcouncil.driver.ClientException;

public interface ClientMode<T>
{
    void init() throws ClientException;

    T startExecutionAndAwaitCompletion() throws ClientException;
}
