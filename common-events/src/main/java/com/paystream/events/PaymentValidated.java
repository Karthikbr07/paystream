package com.paystream.events;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentValidated(
        String paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        Instant validatedAt) {}