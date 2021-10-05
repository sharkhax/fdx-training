package com.drobot.kstream.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class JsonDeserializer<T> implements Deserializer<T> {

    private final Class<T> tClass;

    public JsonDeserializer(Class<T> tClass) {
        this.tClass = tClass;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (tClass == null) {
            throw new RuntimeException("Provided object's class is null");
        }
        if (bytes == null) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(bytes, tClass);
        } catch (IOException e) {
            throw new DeserializationException("Unable to deserialize json to object", e);
        }
    }
}
