package com.example.phoenix;

import com.example.phoenix.models.Insights;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class InsightsProcessorTest {

    InsightsProcessor insightsProcessor;

    @BeforeEach
    public void setup() {
        insightsProcessor = new InsightsProcessor();
    }

    @Test
    public void testWhenGivenInsightsThenCalculateInsightMetricsReturnsDeepCopyWithNewMetrics() {
        final Insights insights = Insights.builder()
                .spend(12.00)
                .clicks(6)
                .impressions(2)
                .fbPurchases(6)
                .phoenixPurchases(6)
                .totalSales(24.00)
                .build();
        final Insights resultInsights = insightsProcessor.calculateInsightMetrics(ImmutableList.of(insights)).get(0);
        Assert.assertEquals(6.00, resultInsights.getCpm(), 0.00);
        Assert.assertEquals(2.00, resultInsights.getCpc(), 0.00);
        Assert.assertEquals(3.00, resultInsights.getCtr(), 0.00);
        Assert.assertEquals(2.00, resultInsights.getFbCpa(), 0.00);
        Assert.assertEquals(2.00, resultInsights.getCpa(), 0.00);
        Assert.assertEquals(1.00, resultInsights.getFbCvr(), 0.00);
        Assert.assertEquals(1.00, resultInsights.getCvr(), 0.00);
        Assert.assertEquals(2.00, resultInsights.getRoas(), 0.00);
        Assert.assertEquals(12.00, resultInsights.getRoi(), 0.00);
    }
}
