package com.paystream.payment_api;

import com.paystream.events.PaymentInitiated;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;


@RestController
@RequestMapping("/payments")
public class PaymentController {

    private static final String TOPIC = "payments.initiated";
    private final KafkaTemplate<String, PaymentInitiated> kafka;

    public PaymentController(KafkaTemplate<String, PaymentInitiated> kafka) {
        this.kafka = kafka;
    }

    public record PaymentRequest(String merchantId, BigDecimal amount, String currency) {}

    @PostMapping
    public String create(@RequestBody PaymentRequest req) {
        String paymentId = UUID.randomUUID().toString();
        var event = new PaymentInitiated(
                paymentId, req.merchantId(), req.amount(), req.currency(), Instant.now());
        kafka.send(TOPIC, paymentId, event); // paymentId is the key
        return paymentId;
    }
}