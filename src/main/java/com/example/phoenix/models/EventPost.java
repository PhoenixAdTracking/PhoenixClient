package com.example.phoenix.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * POJO class modeling an Event Post request.
 */
@Data
@Builder(toBuilder = true)
@AllArgsConstructor
public class EventPost {

    /**
     * The id of the user who instigated the event.
     */
    private final Long clientId;

    /**
     * The id of the ad that was interacted with.
     */
    private final String adId;

    /**
     * The id of the adset that was interacted with.
     */
    private final String adsetId;

    /**
     * The id of the campaign that was interacted with.
     */
    private final String campaignId;

    /**
     * The ip address that this event originated from.
     */
    private final String ipAddress;

    /**
     * The event type.
     */
    private final EventType eventType;

    /**
     * Id of the customer that initiated the interaction
     */
    private final Long customerId;

    /**
     * Email address of the customer.
     */
    private final String email;
}
