package com.ldbc.driver.client;

import com.ldbc.driver.ClientException;

public abstract class ClientMode
{
    private ClientModeType clientModeType;

    public ClientMode(ClientModeType clientModeType) {
        this.clientModeType = clientModeType;

    }

    public ClientModeType getClientModeType() {
        return clientModeType;
    }

    public void init() throws ClientException {

    }

    public void startExecutionAndAwaitCompletion() throws ClientException {
    }
}
