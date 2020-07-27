package com.course.kafka.broker.stream.inventory;

import com.course.kafka.broker.message.InventoryMessage;
import com.course.kafka.util.InventoryTimestampExtractor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

//@Configuration
public class InventoryFiveStream {
    
    @Bean
    public KStream<String, InventoryMessage> kstreamInventory(StreamsBuilder builder){
        var stringSerde = Serdes.String();
        var inventorySerde = new JsonSerde<>(InventoryMessage.class);
        var longSerde = Serdes.Long();

        var windowLenght = Duration.ofHours(1);
        var windowSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowLenght.toMillis());

        var inventoryTimestampExtractor = new InventoryTimestampExtractor();

        var inventoryStream = builder.stream("t.commodity.inventory",
                Consumed.with(stringSerde, inventorySerde, inventoryTimestampExtractor, null));

        inventoryStream.mapValues((k,v)-> v.getType().equalsIgnoreCase("ADD") ? v.getQuantity(): -1 * v.getQuantity())
            .groupByKey().windowedBy(TimeWindows.of(windowLenght))
            .reduce(Long::sum, Materialized.with(stringSerde, longSerde))
            .toStream().through("t.commodity.inventory-total-five", Produced.with(windowSerde, longSerde))
            .print(Printed.toSysOut());

        return inventoryStream;
    }

}
