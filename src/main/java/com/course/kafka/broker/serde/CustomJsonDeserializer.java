package com.course.kafka.broker.serde;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Objects;

public class CustomJsonDeserializer<T> implements Deserializer<T> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> deserializedClass;

    public CustomJsonDeserializer(Class<T> deserializedClass){
        Objects.requireNonNull(deserializedClass, "Deserialized class must not be null");
        this.deserializedClass = deserializedClass;
    }

    @Override
    public T deserialize(String s, byte[] data) {
        try {
            return objectMapper.readValue(data, deserializedClass);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}