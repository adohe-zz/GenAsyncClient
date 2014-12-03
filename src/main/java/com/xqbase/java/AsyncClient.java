package com.xqbase.java;

import java.io.Closeable;
import java.io.IOException;

public class AsyncClient implements Closeable {

    private PoolingNHttpClientConnectionManager connectionManager;

    @Override
    public void close() throws IOException {

    }
}
