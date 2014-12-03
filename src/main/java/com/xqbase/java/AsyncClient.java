package com.xqbase.java;

import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

public class AsyncClient implements Closeable {

    public static final int DEFAULT_MAX_CONNECTIONS = 10;
    public static final int DEFAULT_SOCKET_TIMEOUT = 10 * 1000;
    public static final int DEFAULT_BUFFER_SIZE = 8192;
    public static final int DEFAULT_MAX_RETRIES = 5;
    public static final int DEFAULT_RETRY_SLEEP_TIME_MILLIS = 1500;

    private int maxConnections = DEFAULT_MAX_CONNECTIONS;
    private int connectTimeout = DEFAULT_SOCKET_TIMEOUT;
    private int responseTimeout = DEFAULT_SOCKET_TIMEOUT;

    private final CloseableHttpAsyncClient httpAsyncClient;
    private PoolingNHttpClientConnectionManager connManager;

    public AsyncClient() throws IOException {

        // Create I/O reactor configuration
        IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                .setIoThreadCount(Runtime.getRuntime().availableProcessors())
                .setConnectTimeout(connectTimeout)
                .setSoTimeout(responseTimeout)
                .build();

        // Create a custom I/O reactor
        ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);

        connManager = new PoolingNHttpClientConnectionManager(ioReactor);

        // Create global request configuration
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.DEFAULT)
                .setExpectContinueEnabled(true)
                .setTargetPreferredAuthSchemes(Arrays.asList(AuthSchemes.NTLM, AuthSchemes.DIGEST))
                .setProxyPreferredAuthSchemes(Arrays.asList(AuthSchemes.BASIC))
                .build();

        httpAsyncClient = HttpAsyncClients.custom()
                .setConnectionManager(connManager)
                .setDefaultRequestConfig(defaultRequestConfig)
                .build();
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        maxConnections = maxConnections < 1 ? DEFAULT_MAX_CONNECTIONS : maxConnections;
        this.maxConnections = maxConnections;
        connManager.setMaxTotal(maxConnections);
    }

    @Override
    public void close() throws IOException {

    }
}
