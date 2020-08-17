package com.example.phoenix.models;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * POJO class modeling an Event Post request.
 */
@Data
@AllArgsConstructor
public class EventPost {

    /**
     * The id of the user who instigated the event.
     */
    private final long clientId;

    /**
     * The id of the ad that was interacted with.
     */
    private final long adId;

    /**
     * The ip address that this event originated from.
     */
    private final String ipAddress;

    /**
     * The event type.
     */
    private final EventType eventType;
}
