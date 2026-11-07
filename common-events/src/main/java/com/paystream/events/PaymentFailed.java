package com.paystream.events;

import java.time.Instant;

public record PaymentFailed(
        String paymentId,
        String reason,
        Instant failedAt) {}