package com.paystream.events;

import java.time.Instant;

public record PaymentRejected(
        String paymentId,
        String reason,
        Instant rejectedAt) {}