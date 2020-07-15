package com.course.kafka.util;

import com.course.kafka.broker.message.OrderMessage;
import org.apache.commons.lang3.StringUtils;

public class CommodityStreamUtil {

    public static OrderMessage maskCreditCard(OrderMessage original){

        var converted = original.copy();
        var maskCreditCard = original.getCreditCardNumber().replaceFirst("\\d{12}", StringUtils.repeat('*', 12));

        converted.setCreditCardNumber(maskCreditCard);
        return converted;
    }


}
