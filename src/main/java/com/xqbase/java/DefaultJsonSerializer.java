package com.xqbase.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DefaultJsonSerializer implements Serializer {

    @Override
    public void serialize(Object obj, OutputStream stream) throws IOException {

    }

    @Override
    public Object deserialize(Class objClass, InputStream stream) throws IOException {
        return null;
    }
}
