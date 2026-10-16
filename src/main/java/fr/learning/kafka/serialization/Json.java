package fr.learning.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class Json {
    private Json() { }
    public static final ObjectMapper MAPPER = JsonMapper.builder().addModule(new JavaTimeModule()).build();
}
