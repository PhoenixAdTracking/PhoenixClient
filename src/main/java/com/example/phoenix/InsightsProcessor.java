package com.example.phoenix;

import com.example.phoenix.models.Insights;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Class for processing insights from external platform and the Phoenix DB.
 */
@NoArgsConstructor
public class InsightsProcessor {

    /**
     * Method for calculating the insight metrics that aren't already present in a platform or phoenix db.
     * @param insights
     * @return
     */
    public List<Insights> calculateInsightMetrics(@NonNull final List<Insights> insights) {
        return insights.stream().map(insight -> {
            final double spend = insight.getSpend();
            final int impressions = insight.getImpressions();
            final int clicks = insight.getClicks();
            final double fbPurchases = insight.getFbPurchases();
            final double purchases = insight.getPhoenixPurchases();
            final double totalSales = insight.getTotalSales();
            return insight.toBuilder()
                    .cpm(spend/impressions)
                    .cpc(spend/clicks)
                    .ctr(clicks/impressions)
                    .fbCpa(spend/fbPurchases)
                    .cpa(spend/purchases)
                    .fbCvr(fbPurchases/clicks)
                    .cvr(purchases/clicks)
                    .roas(totalSales/spend)
                    .roi(totalSales - spend)
                    .build();
        }).collect(Collectors.toList());
    }
}
