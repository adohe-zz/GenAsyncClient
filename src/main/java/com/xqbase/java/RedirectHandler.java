package com.xqbase.java;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.protocol.HttpContext;

public class RedirectHandler implements RedirectStrategy {

    private final RequestConfig requestConfig;

    public RedirectHandler(final RequestConfig requestConfig) {
        super();
        this.requestConfig = requestConfig;
    }

    @Override
    public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
        if (!requestConfig.isRedirectsEnabled())
            return false;

        return false;
    }

    @Override
    public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
        return null;
    }
}
