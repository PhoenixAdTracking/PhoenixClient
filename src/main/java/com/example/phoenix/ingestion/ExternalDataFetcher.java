package com.example.phoenix.ingestion;

import com.example.phoenix.models.InsightType;
import com.example.phoenix.models.Insights;
import com.facebook.ads.sdk.*;
import com.google.common.collect.ImmutableList;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public class ExternalDataFetcher {

    /**
     * Set of fields to be pulled for Ad Campaign Insights.
     */
    private static final List<String> FB_CAMPAIGN_INSIGHT_FIELDS =
            ImmutableList.of(
                    "campaign_name",
                    "campaign_id",
                    "frequency",
                    "spend",
                    "impressions",
                    "clicks",
                    "actions");

    /**
     * Set of fields to be pulled for Ad Set Insights.
     */
    private static final List<String> FB_AD_SET_INSIGHT_FIELDS =
            ImmutableList.of(
                    "adset_name",
                    "adset_id",
                    "frequency",
                    "spend",
                    "impressions",
                    "clicks",
                    "actions");

    /**
     * Set of fields to be pulled for Ad Insights.
     */
    private static final List<String> FB_AD_INSIGHT_FIELDS =
            ImmutableList.of(
                    "ad_name",
                    "ad_id",
                    "frequency",
                    "spend",
                    "impressions",
                    "clicks",
                    "actions");

    /**
     * Method for pulling a user's Ad Accounts and returning them as a Map of the account's name to its Id.
     * @param accessToken The access token for pulling this user's info.
     * @param userId The id of the User whose ad accounts need to be pulled.
     * @return A map of ad accounts' names to ids.
     * @throws Exception
     */
    public Map<String, String> getFacebookAdAccounts(
            @NonNull final String accessToken,
            @NonNull final String userId) throws Exception{
        final APIContext context = new APIContext(accessToken);
        final User user = new User(userId, context);
        final List<AdAccount> adAccounts = user.getAdAccounts().requestNameField().requestIdField().execute();
        return adAccounts.stream()
                .collect(Collectors.toMap(
                        adAccount ->
                                adAccount.getFieldName(),
                        adAccount ->
                                adAccount.getFieldId()));

    }

    /**
     * Method for pulling an Ad Account's ad campaigns and their relevant metrics.
     * @param accessToken The Access Token needed for requesting a user's ad information.
     * @param adAccountId The Id of the ad account to pull Campaign info for.
     * @return A list of Insights objects.
     * @throws Exception
     */
    public List<Insights> getAdCampaigns(
            @NonNull final String accessToken,
            @NonNull final String adAccountId) throws Exception{
        final APIContext context = new APIContext(accessToken);
        final AdAccount adAccount = new AdAccount(adAccountId, context);
        return adAccount.getInsights()
                .setLevel("campaign")
                .setDatePreset(AdsInsights.EnumDatePreset.VALUE_LIFETIME)
                .requestFields(FB_CAMPAIGN_INSIGHT_FIELDS)
                .execute().stream()
                .map(insights -> Insights.builder()
                            .type(InsightType.CAMPAIGN)
                            .name(insights.getFieldCampaignName())
                            .id(insights.getFieldCampaignId())
                            .spend(Double.valueOf(insights.getFieldSpend()))
                            .frequency(Double.valueOf(insights.getFieldFrequency()))
                            .impressions(Integer.valueOf(insights.getFieldImpressions()))
                            .clicks(Integer.valueOf(insights.getFieldClicks()))
                            .fbPurchases(getPurchasesFromInsights(insights))
                            .build())
                .collect(Collectors.toList());
    }

    /**
     * Method for pulling an Ad Campaign's ad sets and their relevant metrics.
     * @param accessToken The Access Token needed for requesting a user's ad information.
     * @param adCampaignId The Id of the ad campaign to pull Ad Set info for.
     * @return A list of Insights objects.
     * @throws Exception
     */
    public List<Insights> getAdSets(
            @NonNull final String accessToken,
            @NonNull final String adCampaignId) throws Exception{
        final APIContext context = new APIContext(accessToken);
        final Campaign campaign = new Campaign(adCampaignId, context);
        return campaign.getInsights()
                .setLevel("adset")
                .setDatePreset(AdsInsights.EnumDatePreset.VALUE_LIFETIME)
                .requestFields(FB_AD_SET_INSIGHT_FIELDS)
                .execute().stream()
                .map(insights -> Insights.builder()
                        .type(InsightType.AD_SET)
                        .name(insights.getFieldAdsetName())
                        .id(insights.getFieldAdsetId())
                        .spend(Double.valueOf(insights.getFieldSpend()))
                        .frequency(Double.valueOf(insights.getFieldFrequency()))
                        .impressions(Integer.valueOf(insights.getFieldImpressions()))
                        .clicks(Integer.valueOf(insights.getFieldClicks()))
                        .fbPurchases(getPurchasesFromInsights(insights))
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Method for pulling an Ad Set's ads and their relevant metrics.
     * @param accessToken The Access Token needed for requesting a user's ad information.
     * @param adSetId The Id of the ad campaign to pull Ad Set info for.
     * @return A list of Insights objects.
     * @throws Exception
     */
    public List<Insights> getAds(
            @NonNull final String accessToken,
            @NonNull final String adSetId) throws Exception{
        final APIContext context = new APIContext(accessToken);
        final AdSet adSet = new AdSet(adSetId, context);
        return adSet.getInsights()
                .setLevel("ad")
                .setDatePreset(AdsInsights.EnumDatePreset.VALUE_LIFETIME)
                .requestFields(FB_AD_INSIGHT_FIELDS)
                .execute().stream()
                .map(insights -> Insights.builder()
                        .type(InsightType.AD)
                        .name(insights.getFieldAdName())
                        .id(insights.getFieldAdId())
                        .spend(Double.valueOf(insights.getFieldSpend()))
                        .frequency(Double.valueOf(insights.getFieldFrequency()))
                        .impressions(Integer.valueOf(insights.getFieldImpressions()))
                        .clicks(Integer.valueOf(insights.getFieldClicks()))
                        .fbPurchases(getPurchasesFromInsights(insights))
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Helper method for extracting the number of purchases from a list of action insights.
     * @param insights the AdsInsights object to pull purchases from.
     * @return integer representing the number of purchases made on this ad object.
     */
    private int getPurchasesFromInsights(@NonNull final AdsInsights insights) {
        return insights.getFieldActions().stream()
                .filter(insight -> insight.getFieldActionType().equals("purchase"))
                .findFirst()
                .map(adsActionStats -> Integer.valueOf(adsActionStats.getFieldValue()))
                .orElse(0);
    }
}
