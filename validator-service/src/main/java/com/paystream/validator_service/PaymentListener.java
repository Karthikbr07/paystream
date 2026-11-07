package com.paystream.validator_service;

import com.paystream.events.PaymentInitiated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentListener {

    @KafkaListener(topics = "payments.initiated")
    public void onPayment(ConsumerRecord<String, PaymentInitiated> rec) {
        System.out.printf("partition=%d offset=%d payment=%s%n",
                rec.partition(), rec.offset(), rec.value());
    }
}