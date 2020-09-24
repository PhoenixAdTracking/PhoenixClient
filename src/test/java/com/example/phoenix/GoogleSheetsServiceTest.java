package com.example.phoenix;

import com.example.phoenix.ingestion.GoogleSheetsService;
import com.example.phoenix.models.InsightType;
import com.example.phoenix.models.Insights;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class GoogleSheetsServiceTest {

    @Test
    public void testWhenGivenInsightsThenGoogleSheetsLoadedWithInsightsData() throws Exception {
        final List<Insights> insights = ImmutableList.of(
                Insights.builder()
                .type(InsightType.CAMPAIGN)
                .id("TestId")
                .name("TestName")
                .spend(1.00)
                .impressions(2)
                .frequency(3.00)
                .clicks(4)
                .cpm(5.00)
                .cpc(6.00)
                .ctr(7.00)
                .fbPurchases(8)
                .phoenixPurchases(9)
                .fbCpa(10.00)
                .cpa(11.00)
                .fbCvr(12.00)
                .cvr(13.00)
                .totalSales(14.00)
                .roas(15.00)
                .roi(16.00)
                .build());
        final GoogleSheetsService service = new GoogleSheetsService();
        service.setSheetInfo(insights);
    }
}
