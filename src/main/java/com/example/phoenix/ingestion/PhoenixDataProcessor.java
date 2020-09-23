package com.example.phoenix.ingestion;

import com.example.phoenix.InsightsProcessor;
import com.example.phoenix.models.Business;
import com.example.phoenix.models.InsightType;
import com.example.phoenix.models.Insights;
import com.example.phoenix.models.User;
import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.security.core.userdetails.UserDetails;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    private final ExternalDataFetcher externalDataFetcher;

    private final InsightsProcessor insightsProcessor;

    public PhoenixDataProcessor (@NonNull final BasicDataSource phoenixDb) {
        this.externalDataFetcher = new ExternalDataFetcher();
        this.insightsProcessor = new InsightsProcessor();
        this.phoenixDb = phoenixDb;
    }

    /**
     * Method for creating a new user.
     * @param user a User POJO object
     * @return the id of the newly created user.
     * @throws SQLException if there is an issue with executing the SQL query.
     */
    public int createUser (@NonNull User user) throws SQLException{
        final String newUserStatement = "INSERT INTO users (username, password, firstname, lastname)"
                        + " VALUES (\"" + user.getUsername() + "\", \"" + user.getPassword() + "\", \"" + user.getFirstname() + "\", \"" + user.getLastname() + "\");";
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
     * Pull a user's information from the database, if present.
     * @param username the username to track down.
     * @return the ResultSet associated with this user.
     */
    public Optional<UserDetails> getUser (final String username) throws SQLException{
        final String getUserQuery = "SELECT * FROM users WHERE userId = \"" + username + "\";";
        final ResultSet queryResult = pullRows(getUserQuery);
        if (!queryResult.next()) {
            return Optional.empty();
        } else {
            final String password = queryResult.getString("password");
            return Optional.of(
                    org.springframework.security.core.userdetails.User
                            .withUsername(username)
                            .password(password)
                            .build());
        }
    }

    public List<Insights> getAdCampaignInsights(
            @NonNull final String adAccountId,
            @NonNull final String accessToken) throws Exception {
        final List<Insights> campaignInsights = externalDataFetcher.getAdCampaigns(accessToken, adAccountId);
        final List<Insights> insightsWithPhoenixMetrics = addPhoenixMetrics(campaignInsights);
        final List<Insights> insightsWithCalculatedMetrics = insightsProcessor.calculateInsightMetrics(insightsWithPhoenixMetrics);

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
    private ResultSet pullRows(@NonNull final String query) throws SQLException{
        final PreparedStatement sqlStatement = phoenixDb.getConnection().prepareStatement(query);
        return sqlStatement.executeQuery();
    }

    private List<Insights> getInsights(
            @NonNull final InsightType insightType,
            @NonNull final String accessKey,
            @NonNull final String id) {

        return ImmutableList.of();
    }
}
