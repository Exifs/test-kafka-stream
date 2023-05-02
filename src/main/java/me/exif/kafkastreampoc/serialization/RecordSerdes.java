package me.exif.kafkastreampoc.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import me.exif.kafkastreampoc.dto.RecordDto;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class RecordSerdes implements Serde<RecordDto> {

    private final ObjectMapper objectMapper;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serde.super.configure(configs, isKey);
    }

    @Override
    public void close() {
        Serde.super.close();
    }

    @Override
    public Serializer<RecordDto> serializer() {
        return new RecordSerializer(objectMapper);
    }

    @Override
    public Deserializer<RecordDto> deserializer() {
        return new RecordDeserializer(objectMapper);
    }
}
