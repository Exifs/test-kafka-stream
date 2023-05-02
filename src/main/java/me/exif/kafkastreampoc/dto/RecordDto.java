package me.exif.kafkastreampoc.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Builder
@Data
@Jacksonized
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class RecordDto {
    private final long id;
    private final long productId;
    private final long userId;
    private final MovementType type;
    private final int quantity;
}
