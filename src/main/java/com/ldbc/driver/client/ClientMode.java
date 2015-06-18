package com.ldbc.driver.client;

import com.ldbc.driver.ClientException;

public interface ClientMode<T>
{
    void init() throws ClientException;

    T startExecutionAndAwaitCompletion() throws ClientException;
}
