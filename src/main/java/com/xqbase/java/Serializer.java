package com.xqbase.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Serializer {

    /**
     * Serialize the given object into the stream.
     * @param obj
     * @param stream
     * @throws IOException
     */
     void serialize(Object obj, OutputStream stream) throws IOException;

    /**
     * Deserialize an object with the given type from the stream
     * @param objClass
     * @param stream
     * @return
     * @throws IOException
     */
    Object deserialize(Class objClass, InputStream stream) throws IOException;
}
