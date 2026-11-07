package com.paystream.processor_service;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Document(collection = "processed_payments")
public record ProcessedPayment(
        @Id String paymentId,
        Instant seenAt) {
}