package com.example.phoenix.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;

/**
 * POJO for formatting a GET campaign info request.
 */
@Data
@Generated
@AllArgsConstructor
public class CampaignsRequest {

    /**
     * The Platform access token required for pulling their metrics.
     */
    private String accessToken;

    /**
     * The id of the Ad Account to pull metrics for.
     */
    private String adAccountId;
}
