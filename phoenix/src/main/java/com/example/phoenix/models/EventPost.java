package com.example.phoenix.models;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * POJO class modeling an Event Post request.
 */
@Data
@AllArgsConstructor
public class EventPost {
    private final long clientId;
    private final long adId;
    private final String ipAddress;
    private final EventType eventType;
}
