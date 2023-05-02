package me.exif.kafkastreampoc.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import me.exif.kafkastreampoc.dto.RecordDto;
import org.apache.kafka.common.serialization.Serializer;

@RequiredArgsConstructor
public class RecordSerializer implements Serializer<RecordDto> {

    private final ObjectMapper objectMapper;

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, RecordDto data) {
        return this.objectMapper.writeValueAsBytes(data);
    }
}
