package com.example.phoenix.ingestion;

import com.example.phoenix.models.InsightType;
import com.example.phoenix.models.Insights;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ExternalDataFetcherIntegrationTest {

    /**
     * Note, this token expires 2 months after September 2nd, 2020.
     */
    final static private String FB_ACCESS_TOKEN = System.getenv("FACEBOOK_TEST_ACCESS_TOKEN");

    /**
     * User Id used to run tests.
     */
    final static private String FB_TEST_USER_ID = System.getenv("FACEBOOK_TEST_USER_ID");

    /**
     * Ad Account Id used to run tests
     */
    final static private String FB_TEST_AD_ACCOUNT_ID = System.getenv("FACEBOOK_TEST_AD_ACCOUNT_ID");

    /**
     * Ad Campaign Id used to run tests.
     */
    final static private String FB_TEST_CAMPAIGN_ID = System.getenv("FACEBOOK_TEST_CAMPAIGN_ID");

    /**
     * Ad Set Id used to run tests.
     */
    final static private String FB_TEST_ADSET_ID = System.getenv("FACEBOOK_TEST_ADSET_ID");

    private ExternalDataFetcher externalDataFetcher = new ExternalDataFetcher();

    @Test
    public void whenGivenUserIdAccessTokenThenGetFacebookAdAccountsReturnsListOfAdAccounts() throws Exception{
        final Map<String, String> testMap = externalDataFetcher.getFacebookAdAccounts(FB_ACCESS_TOKEN, FB_TEST_USER_ID);
        Assert.assertFalse(testMap.isEmpty());
        Assert.assertTrue(testMap.containsKey("Genesis Allure LLC 2"));
    }

    @Test
    public void whenGivenAccountIdAndAccessTokenThenGetAdCampaignsReturnsInsights() throws Exception {
        final List<Insights> insights = externalDataFetcher.getAdCampaigns(FB_ACCESS_TOKEN, FB_TEST_AD_ACCOUNT_ID);
        final Insights testInsight = Insights.builder()
                .type(InsightType.CAMPAIGN)
                .name("Genesis Allure Test")
                .id("23844130192950218")
                .spend(98.23)
                .impressions(2227)
                .clicks(119)
                .frequency(1.06606)
                .build();
        Assert.assertTrue(insights.contains(testInsight));
    }

    @Test
    public void whenGivenCampaignIdAndAccessTokenThenGetAdSetsReturnsInsights() throws Exception {
        final List<Insights> insights = externalDataFetcher.getAdSets(FB_ACCESS_TOKEN, FB_TEST_CAMPAIGN_ID);
        final Insights testInsight = Insights.builder()
                .type(InsightType.AD_SET)
                .name("Clinique | 18+ | US | MF")
                .id("23844509413440218")
                .spend(29.61)
                .frequency(1.064516)
                .impressions(1320)
                .clicks(97)
                .build();
        Assert.assertTrue(insights.contains(testInsight));
    }

    @Test
    public void whenGivenAdSetIdAndAccessTokenThenGetAdsReturnsInsights() throws Exception {
        final List<Insights> insights = externalDataFetcher.getAds(FB_ACCESS_TOKEN, FB_TEST_ADSET_ID);
        final Insights testInsight = Insights.builder()
                .type(InsightType.AD)
                .name("Ad Copy #3")
                .id("23844509413430218")
                .spend(29.61)
                .frequency(1.064516)
                .impressions(1320)
                .clicks(97)
                .build();
        Assert.assertTrue(insights.contains(testInsight));
    }
}
