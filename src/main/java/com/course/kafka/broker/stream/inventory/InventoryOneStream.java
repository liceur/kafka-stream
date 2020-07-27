package com.course.kafka.broker.stream.inventory;

import com.course.kafka.broker.message.InventoryMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class InventoryOneStream {

    public KStream<String, InventoryMessage> kstreamInventory(StreamsBuilder streamBuilder){
        var stringSerde = Serdes.String();
        var inventorySerde = new JsonSerde<>(InventoryMessage.class);
        var longSerde = Serdes.Long();

        var inventoryStream = streamBuilder.stream("t.commodity.inventory", Consumed.with(stringSerde, inventorySerde));

        inventoryStream.mapValues((k,v)-> v.getQuantity()).groupByKey()
                .aggregate(() -> 0l, (aggKey, newValue, aggValue) -> aggValue + newValue,
                        Materialized.with(stringSerde, longSerde))
                .toStream().to("t.commodity.inventory-total-one", Produced.with(stringSerde, longSerde));

        return inventoryStream;
    }

}
