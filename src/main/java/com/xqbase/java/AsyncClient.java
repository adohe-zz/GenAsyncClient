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

    public static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 10;
    private static final int MAX_CONN_MULTIPLICATION = 10;
    public static final int DEFAULT_SOCKET_TIMEOUT = 10 * 1000;
    public static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT = 10 * 1000;
    public static final int DEFAULT_CONNECT_TIMEOUT = 10 * 1000;
    public static final int DEFAULT_BUFFER_SIZE = 8192;
    public static final int DEFAULT_MAX_RETRIES = 5;
    public static final int DEFAULT_RETRY_SLEEP_TIME_MILLIS = 1500;

    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private int socketTimeout = DEFAULT_SOCKET_TIMEOUT;
    private int connectionRequestTimeout = DEFAULT_CONNECTION_REQUEST_TIMEOUT;
    private int defaultMaxPerRoute = DEFAULT_MAX_CONNECTIONS_PER_ROUTE;

    private final CloseableHttpAsyncClient httpAsyncClient;
    private PoolingNHttpClientConnectionManager connManager;

    public AsyncClient() throws IOException {

        // Create I/O reactor configuration
        IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                .setIoThreadCount(Runtime.getRuntime().availableProcessors())
                .setTcpNoDelay(true)
                .build();

        // Create a custom I/O reactor
        ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);

        // Create a custom Connection Manager
        connManager = new PoolingNHttpClientConnectionManager(ioReactor);
        connManager.setDefaultMaxPerRoute(defaultMaxPerRoute);
        connManager.setMaxTotal(defaultMaxPerRoute * MAX_CONN_MULTIPLICATION);

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

    /**
     * Get maximum connections per route allowed
     * @return maximum connections per route allowed
     */
    public int getDefaultMaxPerRoute() {
        return defaultMaxPerRoute;
    }

    /**
     * Determines the maximum connections per route allowed
     * @param defaultMaxPerRoute maximum connections value
     */
    public void setDefaultMaxPerRoute(int defaultMaxPerRoute) {
        defaultMaxPerRoute = defaultMaxPerRoute < 1 ? DEFAULT_MAX_CONNECTIONS_PER_ROUTE : defaultMaxPerRoute;
        this.defaultMaxPerRoute = defaultMaxPerRoute;
        this.connManager.setDefaultMaxPerRoute(this.defaultMaxPerRoute);
        this.connManager.setMaxTotal(this.defaultMaxPerRoute * MAX_CONN_MULTIPLICATION);
    }

    /**
     * Determines the timeout in milliseconds until a connection is established.
     * A timeout value of zero is interpreted as an infinite timeout.
     * <p>
     * A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     * </p>
     * <p>
     * Default: {@code -1}
     * </p>
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Set the connection timeout
     * @param connectTimeout connect timeout
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Defines the socket timeout ({@code SO_TIMEOUT}) in milliseconds,
     * which is the timeout for waiting for data  or, put differently,
     * a maximum period inactivity between two consecutive data packets).
     * <p>
     * A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     * </p>
     * <p>
     * Default: {@code -1}
     * </p>
     */
    public int getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * Set the socket timeout
     * @param socketTimeout socket timeout
     */
    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    /**
     * Returns the timeout in milliseconds used when requesting a connection
     * from the connection manager. A timeout value of zero is interpreted
     * as an infinite timeout.
     * <p>
     * A timeout value of zero is interpreted as an infinite timeout.
     * A negative value is interpreted as undefined (system default).
     * </p>
     * <p>
     * Default: {@code -1}
     * </p>
     */
    public int getConnectionRequestTimeout() {
        return connectionRequestTimeout;
    }

    /**
     * Set the connection request timeout
     * @param connectionRequestTimeout connection request timeout
     */
    public void setConnectionRequestTimeout(int connectionRequestTimeout) {
        this.connectionRequestTimeout = connectionRequestTimeout;
    }

    @Override
    public void close() throws IOException {
        httpAsyncClient.close();
    }
}
