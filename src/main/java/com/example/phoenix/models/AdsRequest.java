package com.example.phoenix.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;

/**
 * POJO for formatting a GET ads info request.
 */
@Data
@Generated
@AllArgsConstructor
public class AdsRequest {

    /**
     * The Platform access token required for pulling their metrics.
     */
    private String accessToken;

    /**
     * The id of the adset to pull metrics for.
     */
    private String adsetId;
}