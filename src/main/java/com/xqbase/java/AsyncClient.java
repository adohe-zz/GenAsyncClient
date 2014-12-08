package com.xqbase.java;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.concurrent.Callable;

public class AsyncClient implements Closeable {

    public static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 10;
    private static final int MAX_CONN_MULTIPLICATION = 10;
    public static final int DEFAULT_SOCKET_TIMEOUT = 10 * 1000;
    public static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT = 10 * 1000;
    public static final int DEFAULT_CONNECT_TIMEOUT = 10 * 1000;
    public static final boolean DEFAULT_REDIRECTS_ENABLED = true;
    public static final int DEFAULT_BUFFER_SIZE = 8192;
    public static final int DEFAULT_MAX_RETRIES = 5;
    public static final int DEFAULT_RETRY_SLEEP_TIME_MILLIS = 1500;

    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private int socketTimeout = DEFAULT_SOCKET_TIMEOUT;
    private int connectionRequestTimeout = DEFAULT_CONNECTION_REQUEST_TIMEOUT;
    private int defaultMaxPerRoute = DEFAULT_MAX_CONNECTIONS_PER_ROUTE;
    private boolean redirectsEnabled = DEFAULT_REDIRECTS_ENABLED;
    private boolean relativeRedirectsAllowed = DEFAULT_REDIRECTS_ENABLED;
    private boolean circularRedirectsAllowed = DEFAULT_REDIRECTS_ENABLED;

    private final CloseableHttpAsyncClient httpAsyncClient;
    private PoolingNHttpClientConnectionManager connManager;
    private RequestConfig requestConfig;
    private RedirectStrategy redirectStrategy;

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

        // Update the request level configuration
        updateRequestConfig();

        // Create a custom Redirect Handler
        redirectStrategy = new RedirectHandler(requestConfig);

        httpAsyncClient = HttpAsyncClients.custom()
                .setConnectionManager(connManager)
                .setDefaultRequestConfig(defaultRequestConfig)
                .setRedirectStrategy(redirectStrategy)
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
        connectTimeout = connectTimeout< 0 ? DEFAULT_CONNECT_TIMEOUT : connectTimeout;
        this.connectTimeout = connectTimeout;
        updateRequestConfig();
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
        socketTimeout = socketTimeout < 0 ? DEFAULT_SOCKET_TIMEOUT : socketTimeout;
        this.socketTimeout = socketTimeout;
        updateRequestConfig();
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
        connectionRequestTimeout = connectionRequestTimeout < 0 ? DEFAULT_CONNECTION_REQUEST_TIMEOUT : connectionRequestTimeout;
        this.connectionRequestTimeout = connectionRequestTimeout;
        updateRequestConfig();
    }

    /**
     * Enable redirects or not
     * @param enableRedirects
     * @param enableRelativeRedirects
     * @param enableCircularRedirects
     */
    public void setEnableRedirects(final boolean enableRedirects, final boolean enableRelativeRedirects, final boolean enableCircularRedirects) {
        this.redirectsEnabled = enableRedirects;
        this.relativeRedirectsAllowed = enableRelativeRedirects;
        this.circularRedirectsAllowed = enableCircularRedirects;
        updateRequestConfig();
        redirectStrategy = new RedirectHandler(requestConfig);
    }

    public void setEnableRedirects(final boolean enableRedirects, final boolean enableRelativeRedirects) {
        setEnableRedirects(enableRedirects, enableRelativeRedirects, true);
    }

    public void setEnableRedirects(final boolean enableRedirects) {
        setEnableRedirects(enableRedirects, enableRedirects, enableRedirects);
    }

    /**
     * Update the request level configuration
     */
    private void updateRequestConfig() {
        requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(connectionRequestTimeout)
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .setRedirectsEnabled(redirectsEnabled)
                .setRelativeRedirectsAllowed(relativeRedirectsAllowed)
                .setCircularRedirectsAllowed(circularRedirectsAllowed)
                .build();
    }

    /**
     * The simple get interface
     * @param url request url
     */
    public void get(String url) {
        get(url, null);
    }

    public void get(String url, RequestParams params) {
        sendRequest(null, new HttpGet(getUrlWithQueryString(false, url, params)));
    }

    /**
     * The simple post interface
     * @param url request url
     * @param entity request body
     */
    public void post(String url, HttpEntity entity) {
        HttpPost post = new HttpPost(url);
        post.setConfig(requestConfig);
        post.setEntity(entity);
        sendRequest(null, post);
    }

    private void sendRequest(HttpClientContext context, HttpUriRequest request) {

    }

    private HttpEntityEnclosingRequestBase addEntityToRequestBase(HttpEntityEnclosingRequestBase requestBase, HttpEntity entity) {
        if (requestBase != null) {
            requestBase.setEntity(entity);
        }

        return requestBase;
    }

    @Override
    public void close() throws IOException {
        httpAsyncClient.close();
    }

    /**
     * Will encode url, if not disabled, and adds params on the end of it
     *
     * @param url             String with URL, should be valid URL without params
     * @param params          RequestParams to be appended on the end of URL
     * @param shouldEncodeUrl whether url should be encoded (replaces spaces with %20)
     * @return encoded url if requested with params appended if any available
     */
    public static String getUrlWithQueryString(boolean shouldEncodeUrl, String url, RequestParams params) {
        if (url == null) {
            return null;
        }

        if (shouldEncodeUrl) {
            try {
                url = URLEncoder.encode(url, "utf-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        if (params != null) {
            String paramString = params.getParamString().trim();

            if (!paramString.equals("") && !paramString.equals("?")) {
                url += url.contains("?") ? "&" : "?";
                url += paramString;
            }
        }

        return url;
    }

    private class AsyncTransformation implements AsyncFunction<HttpResponse, Object> {

        private final ListeningExecutorService transformPool;
        private final Class clazz;

        private Serializer serializer = new DefaultJsonSerializer();

        public AsyncTransformation(final ListeningExecutorService transformPool, Class clazz) {
            this.transformPool = transformPool;
            this.clazz = clazz;
        }

        @Override
        public ListenableFuture<Object> apply(HttpResponse response) throws Exception {
            return this.transformPool.submit(new TransformWorker(response, this.clazz));
        }

        private void close(HttpResponse response) {
            if (response == null) {
                return;
            }

            HttpEntity entity = response.getEntity();
            if (entity != null) {
                EntityUtils.consumeQuietly(entity);
            }
        }

        private class TransformWorker implements Callable {

            private final HttpResponse response;
            private final Class clazz;

            public TransformWorker(final HttpResponse response, final Class clazz) {
                this.response = response;
                this.clazz = clazz;
            }

            @Override
            public Object call() throws Exception {
                Object obj = serializer.deserialize(clazz, response.getEntity().getContent());
                close(response);
                return obj;
            }
        }
    }
}
