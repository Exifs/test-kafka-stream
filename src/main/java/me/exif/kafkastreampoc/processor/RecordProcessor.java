package me.exif.kafkastreampoc.processor;

import lombok.RequiredArgsConstructor;
import me.exif.kafkastreampoc.aggregator.StocksAggregator;
import me.exif.kafkastreampoc.serialization.RecordSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RecordProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private final RecordSerdes recordSerde;
    public static final String ITEMS_STOCK_TABLE_NAME = "items_stock";
    public static final String INPUT_TOPIC = "stocks-events";
    public static final String OUTPUT_TOPIC = "items-stock";

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KTable<Long, Integer> kTable = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(STRING_SERDE, this.recordSerde))
                .map((key, value) -> new KeyValue<>(value.getProductId(), value))
                .groupByKey(Grouped.with(Serdes.Long(), this.recordSerde))
                .aggregate(() -> 0, new StocksAggregator(), Materialized.as(ITEMS_STOCK_TABLE_NAME));
        kTable.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Integer()));
    }
}