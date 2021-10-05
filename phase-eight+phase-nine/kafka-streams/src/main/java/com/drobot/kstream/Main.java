package com.drobot.kstream;

import com.drobot.kstream.processor.ExtendedToLightBookingsMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class Main {

    private static final ApplicationProperties PROPERTIES = ApplicationProperties.getInstance();

    public static void main(String[] args) {
        Topology topology = buildApplicationTopology();
        KafkaStreams streams = new KafkaStreams(topology, PROPERTIES);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Topology buildApplicationTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        String subscribeTopic = PROPERTIES.getBookingsTopic();
        String produceTopic = PROPERTIES.getLightBookingsTopic();
        KStream<String, String> initialBookings = subscribe(builder, subscribeTopic);
        KStream<String, String> lightBookings = transformToLightForm(initialBookings);
        produce(lightBookings, produceTopic);
        return builder.build();
    }

    private static KStream<String, String> subscribe(StreamsBuilder builder, String topic) {
        return builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));
    }

    private static KStream<String, String> transformToLightForm(KStream<String, String> extendedBookings) {
        return extendedBookings.mapValues(value -> new ExtendedToLightBookingsMapper().apply(value));
    }

    private static void produce(KStream<String, String> input, String topic) {
        input.to(topic, Produced.with(Serdes.String(), Serdes.String()));
    }
}
