package me.exif.kafkastreampoc.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.exif.kafkastreampoc.dto.RecordDto;
import org.apache.kafka.common.serialization.Deserializer;

@RequiredArgsConstructor
@Slf4j
public class RecordDeserializer implements Deserializer<RecordDto> {
    private final ObjectMapper objectMapper;

    @SneakyThrows
    @Override
    public RecordDto deserialize(String topic, byte[] data) {
        return this.objectMapper.readValue(data, RecordDto.class);
    }
}
