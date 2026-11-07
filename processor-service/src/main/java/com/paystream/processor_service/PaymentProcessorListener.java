package com.paystream.processor_service;

import com.paystream.events.PaymentFailed;
import com.paystream.events.PaymentProcessed;
import com.paystream.events.PaymentValidated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Component
public class PaymentProcessorListener {

    private static final BigDecimal LIMIT = new BigDecimal("50000");

    private final KafkaTemplate<String, Object> kafka;
    private final MongoTemplate mongo;

    public PaymentProcessorListener(KafkaTemplate<String, Object> kafka, MongoTemplate mongo) {
        this.kafka = kafka;
        this.mongo = mongo;
    }

    @KafkaListener(topics = "payments.validated")
    public void onValidated(ConsumerRecord<String, PaymentValidated> rec) {
        PaymentValidated p = rec.value();

        // Idempotency check: first writer wins, a second insert fails
        try {
            mongo.insert(new ProcessedPayment(p.paymentId(), Instant.now()));
        } catch (DuplicateKeyException e) {
            System.out.println("DUPLICATE skipped " + p.paymentId());
            return;
        }

        if (p.amount().compareTo(LIMIT) > 0) {
            kafka.send("payments.failed", p.paymentId(),
                    new PaymentFailed(p.paymentId(), "exceeds transaction limit", Instant.now()));
            System.out.println("FAILED    " + p.paymentId());
        } else {
            String txnId = "TXN-" + UUID.randomUUID();
            kafka.send("payments.processed", p.paymentId(),
                    new PaymentProcessed(p.paymentId(), p.merchantId(), p.amount(),
                            p.currency(), txnId, Instant.now()));
            System.out.println("PROCESSED " + p.paymentId() + " -> " + txnId);
        }
    }
}