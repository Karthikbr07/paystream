package com.paystream.events;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentInitiated(
        String paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        Instant createdAt) {}