package com.example.phoenix.ingestion;

import com.example.phoenix.InsightsProcessor;
import com.example.phoenix.models.Business;
import com.example.phoenix.models.InsightType;
import com.example.phoenix.models.Insights;
import com.example.phoenix.models.User;
import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.security.crypto.password.PasswordEncoder;

import javax.annotation.Resource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Class used to organize all data ingestion business logic.
 */
public class PhoenixDataProcessor {

    /**
     * Title of the users table Id column.
     */
    private static final String USER_ID_COLUMN = "userId";

    /**
     * Title of the businesses table Id column
     */
    private static final String BUSINESS_ID_COLUMN = "businessId";

    /**
     * Title of the user to businesses table Id column
     */
    private static final String USER_TO_BUSINESS_ID_COLUMN = "businessId";

    /**
     * Access point for the Phoenix DB.
     */
    private final BasicDataSource phoenixDb;

    @Resource (name = "PasswordEncoder")
    private PasswordEncoder passwordEncoder;

    private final ExternalDataFetcher externalDataFetcher;

    private final InsightsProcessor insightsProcessor;

    private final GoogleSheetsService service;

    public PhoenixDataProcessor (@NonNull final BasicDataSource phoenixDb) {
        this.externalDataFetcher = new ExternalDataFetcher();
        this.insightsProcessor = new InsightsProcessor();
        this.phoenixDb = phoenixDb;
        this.service = new GoogleSheetsService();
    }

    /**
     * Method for creating a new user.
     * @param user a User POJO object
     * @return the id of the newly created user.
     * @throws SQLException if there is an issue with executing the SQL query.
     */
    public int createUser (@NonNull User user) throws SQLException {
        final String newUserStatement = "INSERT INTO users (username, password, firstname, lastname) VALUES (\""
                + user.getUsername() + "\", \""
                + passwordEncoder.encode(user.getPassword()) + "\", \""
                + user.getFirstname() + "\", \""
                + user.getLastname() + "\");";
        final int userId = insertRow(newUserStatement, USER_ID_COLUMN);
        final String userToBusinessStatement = "INSERT INTO user_to_business (userId, businessId, active, role)"
                + " VALUES (" + userId + ", " + user.getBusinessId() + ", 1, \"" + user.getRole() + "\");";
        insertRow(userToBusinessStatement, USER_TO_BUSINESS_ID_COLUMN);
        return userId;
    }

    /**
     * Method for creating a new business.
     * @param business a Business POJO object
     * @return the id of the newly created business.
     * @throws SQLException if there is an issue with executing the SQL query.
     */
    public int createBusiness (@NonNull Business business) throws SQLException{
        final String newBusinessStatement = "INSERT INTO businesses (name) VALUES (\"" + business.getName() + "\");";
        return insertRow(newBusinessStatement, BUSINESS_ID_COLUMN);
    }

    /**
     * Method for pulling a complete list of all Insights related to an Ad Account's Campaigns.
     * @param adObjectId the id of the ad account to pull campaign insights for.
     * @param accessToken access token required to pull platform insights.
     * @param type the type of ad object to pull insights for.
     * @return List of Insights.
     * @throws Exception
     */
    public List<Insights> getInsights(
            @NonNull final String adObjectId,
            @NonNull final String accessToken,
            @NonNull final InsightType type) throws Exception {
        final List<Insights> platformInsights;
        switch (type) {
            case AD:
                platformInsights = externalDataFetcher.getAds(accessToken, adObjectId);
                break;
            case AD_SET:
                platformInsights = externalDataFetcher.getAdSets(accessToken, adObjectId);
                break;
            case CAMPAIGN:
            default:
                platformInsights = externalDataFetcher.getAdCampaigns(accessToken, adObjectId);
                break;
        }
        final List<Insights> insightsWithPhoenixMetrics = addPhoenixMetrics(platformInsights);
        final List<Insights> insightsWithCalculatedMetrics = insightsProcessor.calculateInsightMetrics(insightsWithPhoenixMetrics);
        service.setSheetInfo(insightsWithCalculatedMetrics);
        return insightsWithCalculatedMetrics;
    }

    /**
     * Method for adding Phoenix tracked event metrics to Insight objects.
     * @param insights the Insights for ad objects.
     * @return a deep copy of the parameter Inputs but with Phoenix metrics added to it.
     */
    public List<Insights> addPhoenixMetrics(@NonNull final List<Insights> insights) {
        final String campaignQuery = "SELECT * FROM adevents WHERE campaignId = {0};";
        final String adsetQuery = "SELECT * FROM adevents WHERE adsetId = {0};";
        final String adQuery = "SELECT * FROM adevents WHERE adId = {0};";
        final InsightType insightType = insights.stream().map(insight -> insight.getType()).findFirst().orElse(null);
        switch (insightType) {
            case AD:
                return addPhoenixMetrics(insights, adQuery);
            case AD_SET:
                return addPhoenixMetrics(insights, adsetQuery);
            case CAMPAIGN:
            default:
                return addPhoenixMetrics(insights, campaignQuery);
        }
    }

    /**
     * Method for adding Phoenix tracked event metrics to Insight objects.
     * @param adsetInsights the Insights for a given ad object.
     * @param queryTemplate the string used to query information for the ad objects from phoenixDB.
     * @return a deep copy of the parameter Inputs but with Phoenix metrics added to it.
     */
    private List<Insights> addPhoenixMetrics(
            @NonNull final List<Insights> adsetInsights,
            @NonNull final String queryTemplate) {
        return adsetInsights.stream()
                .map(insights -> {
                    int purchaseCounter = 0;
                    double totalAmount = 0.00;
                    try (ResultSet resultSet = pullRows(MessageFormat.format(queryTemplate, insights.getId()))){
                        while (resultSet.next()) {
                            final String interactionType = resultSet.getString("type");
                            if (interactionType.equals("purchase")) {
                                purchaseCounter++;
                                totalAmount += Double.valueOf(resultSet.getString("purchaseAmount"));
                            }
                        }
                        return insights.toBuilder()
                                .phoenixPurchases(purchaseCounter)
                                .totalSales(totalAmount)
                                .build();
                    } catch (SQLException sqle) {
                        return insights;
                    }
                })
                .collect(Collectors.toList());
    }

    /**
     * Helper function for running insert queries and returning the id
     * @param query the query to run to insert new data into the database.
     * @param idColumn the name of the column that holds the ID for this table.
     * @return the id number for the newly inserted row.
     * @throws SQLException if there is an issue with executing the SQL query.
     */
    private int insertRow(
            @NonNull final String query,
            @NonNull final String idColumn) throws SQLException{
        final PreparedStatement sqlStatement = phoenixDb.getConnection().prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        sqlStatement.executeUpdate();
        final ResultSet resultSet = sqlStatement.getGeneratedKeys();
        resultSet.next();
        return resultSet.getInt(idColumn);

    }

    /**
     * Helper function for obtaining rows from a table based on some query.
     * @param query the query to run to pull data from the database.
     * @return ResultSet of the query.
     * @throws SQLException if there is an issue with executing the SQL query.
     */
    private ResultSet pullRows(@NonNull final String query) {
        try {

            return phoenixDb.getConnection().prepareStatement(query).executeQuery();
        } catch (SQLException sqle) {
            System.out.println("Exception");
            System.out.println(sqle.getMessage());
            return null;
        }
    }
}
