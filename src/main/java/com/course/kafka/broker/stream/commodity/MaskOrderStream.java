package com.course.kafka.broker.stream.commodity;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.util.CommodityStreamUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import javax.print.attribute.standard.OrientationRequested;

@Configuration
public class MaskOrderStream {

    public KStream<String, OrderMessage> kstreamCommodityTrading(StreamsBuilder builder){
        var stringSerde = Serdes.String();
        var orderSerder = new JsonSerde<>(OrderMessage.class);

        KStream<String, OrderMessage> maskedOrderStream = builder.stream("t.commodity.order", Consumed.with(stringSerde,orderSerder))
                .mapValues(CommodityStreamUtil::maskCreditCard);

        maskedOrderStream.to("t.commodity.order-masked", Produced.with(stringSerde, orderSerder));
        maskedOrderStream.print(Printed.<String, OrderMessage>toSysOut().withLabel("Masked Order Strean"));

        return maskedOrderStream;
    }

}
