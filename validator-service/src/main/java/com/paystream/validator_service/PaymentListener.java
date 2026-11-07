package com.paystream.validator_service;

import com.paystream.events.PaymentInitiated;
import com.paystream.events.PaymentRejected;
import com.paystream.events.PaymentValidated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;

@Component
public class PaymentListener {

    private static final Set<String> SUPPORTED_CURRENCIES = Set.of("INR", "USD", "EUR");

    private final KafkaTemplate<String, Object> kafka;

    public PaymentListener(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    @KafkaListener(topics = "payments.initiated")
    public void onPayment(ConsumerRecord<String, PaymentInitiated> rec) {
        PaymentInitiated p = rec.value();
        String problem = check(p);

        if (problem == null) {
            kafka.send("payments.validated", p.paymentId(),
                    new PaymentValidated(p.paymentId(), p.merchantId(),
                            p.amount(), p.currency(), Instant.now()));
            System.out.println("VALIDATED " + p.paymentId());
        } else {
            kafka.send("payments.rejected", p.paymentId(),
                    new PaymentRejected(p.paymentId(), problem, Instant.now()));
            System.out.println("REJECTED  " + p.paymentId() + " -> " + problem);
        }
    }

    private String check(PaymentInitiated p) {
        if (p.merchantId() == null || p.merchantId().isBlank()) return "merchantId is missing";
        if (p.amount() == null || p.amount().compareTo(BigDecimal.ZERO) <= 0) return "amount must be greater than zero";
        if (p.currency() == null || !SUPPORTED_CURRENCIES.contains(p.currency())) return "unsupported currency";
        return null; // null means "no problems found"
    }
}