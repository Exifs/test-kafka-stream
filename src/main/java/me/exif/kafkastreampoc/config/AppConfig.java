package me.exif.kafkastreampoc.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class AppConfig {

    @Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.state-directory:/tmp/testkafka/store}")
    private String stateDirectory;

    @Bean
    public ObjectMapper getObjectMapper() {
        return new ObjectMapper();
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig(HostInfo hostInfo) {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "stock-ktable-aggregator");
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

        // be carefull while local testing,
        // delete this folder between resets
        // to be sure that the Ktable will be correctly reset instead of adding your current stream to the cache
        props.put(STATE_DIR_CONFIG, stateDirectory);

        // wierdly are overwritting any types in KTable aggregate
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());

        props.put(APPLICATION_SERVER_CONFIG, hostInfo.host() + ":" + hostInfo.port());

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public HostInfo currentServiceHostInfo(@Value("${server.port}") int portNumber) throws UnknownHostException {
        String hostname = "localhost";
        log.info("Current host seems to be: " + hostname + ":" + portNumber);
        return new HostInfo(hostname, portNumber);
    }

}