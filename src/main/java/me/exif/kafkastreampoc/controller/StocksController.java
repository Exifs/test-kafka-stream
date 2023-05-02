package me.exif.kafkastreampoc.controller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import static me.exif.kafkastreampoc.processor.RecordProcessor.ITEMS_STOCK_TABLE_NAME;

@Controller
@RequestMapping("/item")
@RequiredArgsConstructor
public class StocksController {

    private final StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/stock/{itemId}")
    public ResponseEntity<Integer> getWordCount(@PathVariable Long itemId) {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();

        if (kafkaStreams == null) return ResponseEntity.notFound().build();

        ReadOnlyKeyValueStore<Long, Integer> stocks = null;
        try {
            stocks = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType(ITEMS_STOCK_TABLE_NAME, QueryableStoreTypes.keyValueStore())
            );
            return ResponseEntity.status(stocks.get(itemId) == null ? HttpStatus.NOT_FOUND: HttpStatus.OK).body(stocks.get(itemId));
        } catch (InvalidStateStoreException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }

    }
}
