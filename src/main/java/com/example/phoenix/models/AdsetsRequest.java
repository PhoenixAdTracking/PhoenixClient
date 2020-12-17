package com.example.phoenix.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;

/**
 * POJO for formatting a GET adset info request.
 */
@Data
@Generated
@AllArgsConstructor
public class AdsetsRequest {

    /**
     * The Platform access token required for pulling their metrics.
     */
    private String accessToken;

    /**
     * The id of the campaign to pull metrics for.
     */
    private String campaignId;
}