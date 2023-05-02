package me.exif.kafkastreampoc.aggregator;

import me.exif.kafkastreampoc.dto.RecordDto;
import org.apache.kafka.streams.kstream.Aggregator;

public class StocksAggregator implements Aggregator<Long, RecordDto, Integer> {
    @Override
    public Integer apply(Long key, RecordDto value, Integer aggregate) {
        switch (value.getType()) {
            case INCREASE:
                return aggregate + value.getQuantity();
            case DECREASE:
                return aggregate - value.getQuantity();
            default:
                throw new IllegalArgumentException();
        }
    }
}
