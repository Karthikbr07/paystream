package com.paystream.events;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentProcessed(
        String paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String transactionId,
        Instant processedAt) {}