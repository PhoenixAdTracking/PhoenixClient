package com.example.phoenix.models;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
public class Insights {
    private InsightType type;
    private String id;
    private String name;
    private double spend;
    private int impressions;
    private double frequency;
    private int clicks;
    private double cpm;
    private double cpc;
    private double ctr;
    private double fbPurchases;
    private double phoenixPurchases;
    private double fbCpa;
    private double cpa;
    private double fbCvr;
    private double cvr;
    private double totalSales;
    private double roas;
    private double roi;
}
