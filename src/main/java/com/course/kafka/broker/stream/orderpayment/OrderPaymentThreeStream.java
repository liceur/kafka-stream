package com.course.kafka.broker.stream.orderpayment;

import com.course.kafka.broker.message.OnlineOrderMessage;
import com.course.kafka.broker.message.OnlineOrderPaymentMessage;
import com.course.kafka.broker.message.OnlinePaymentMessage;
import com.course.kafka.util.OnlineOrderTimestampExtractor;
import com.course.kafka.util.OnlinePaymentTimestampExtractor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

@Configuration
public class OrderPaymentThreeStream {

    @Bean
    public KStream<String, OnlineOrderMessage> kstreamOrderPayment(StreamsBuilder builder){
        var stringSerde = Serdes.String();
        var orderSerde = new JsonSerde<>(OnlineOrderMessage.class);
        var paymentSerde = new JsonSerde<>(OnlinePaymentMessage.class);
        var orderPaymentSerde = new JsonSerde<>(OnlineOrderPaymentMessage.class);


        var orderStream = builder.stream("t.commodity.online-order",
                Consumed.with(stringSerde, orderSerde, new OnlineOrderTimestampExtractor(), null));
        var paymentStream = builder.stream("t.commodity.online-payment",
                Consumed.with(stringSerde, paymentSerde, new OnlinePaymentTimestampExtractor(), null));


        // join
        orderStream
                .outerJoin(paymentStream, this::joinOrderPayment, JoinWindows.of(Duration.ofHours(1)),
                StreamJoined.with(stringSerde, orderSerde, paymentSerde))
                .to("t.commodity.join-order-payment-three", Produced.with(stringSerde, orderPaymentSerde));

        return orderStream;
    }

    private OnlineOrderPaymentMessage joinOrderPayment(OnlineOrderMessage order, OnlinePaymentMessage payment){
        var result = new OnlineOrderPaymentMessage();

        if ( order != null){
            result.setOnlineOrderNumber(order.getOnlineOrderNumber());
            result.setOrderDateTime(order.getOrderDateTime());
            result.setTotalAmount(order.getTotalAmount());
            result.setUsername(order.getUserName());
        }

        if ( payment != null) {
            result.setPaymentDateTime(payment.getPaymentDateTime());
            result.setPaymentMethod(payment.getPaymentMethod());
            result.setPaymentNumber(payment.getPaymentNumber());
        }
        return result;

    }

}
