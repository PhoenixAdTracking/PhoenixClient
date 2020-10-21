package com.example.phoenix.ingestion;

import com.example.phoenix.InsightsProcessor;
import com.example.phoenix.MissingEventInfoException;
import com.example.phoenix.models.*;
import lombok.NonNull;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import javax.xml.transform.Result;
import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
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
    private final Connection phoenixConn;

//    @Resource (name = "PasswordEncoder")
    private PasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

    /**
     * Object used to pull data from various ad host platforms.
     */
    private final ExternalDataFetcher externalDataFetcher;

    /**
     * Processor for calculating second-degree metrics from platforms.
     */
    private final InsightsProcessor insightsProcessor;

    /**
     * Service for inserting metrics into a google sheet.
     */
    private final GoogleSheetsService service;

    public PhoenixDataProcessor (@NonNull final Connection phoenixConn) {
        this.externalDataFetcher = new ExternalDataFetcher();
        this.insightsProcessor = new InsightsProcessor();
        this.phoenixConn = phoenixConn;
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
        final int userId = insertRow(newUserStatement);
        final String userToBusinessStatement = "INSERT INTO user_to_business (userId, businessId, active, role)"
                + " VALUES (" + userId + ", " + user.getBusinessId() + ", 1, \"" + user.getRole() + "\");";
        insertRow(userToBusinessStatement);
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
        return insertRow(newBusinessStatement);
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
        final String campaignQuery = "SELECT * FROM ad_events WHERE campaignId = {0};";
        final String adsetQuery = "SELECT * FROM ad_events WHERE adsetId = {0};";
        final String adQuery = "SELECT * FROM ad_events WHERE adId = {0};";
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
     * Helper method for processing a click ad-event and storing it in the database.
     * If the event has no ip address attached, an exception is thrown.
     * If the event has no user attached, a new one is created.
     * @param event the click event to process
     * @return The customer Id associated with the event.
     * @throws MissingEventInfoException
     * @throws SQLException
     */
    public long processClickEvent (@NonNull final EventPost event) throws MissingEventInfoException, SQLException {
        validateClickEventIsValid(event);
        final long customerId;
        final long ipAddressId = registerIpAddress(event.getIpAddress());
        if (event.getCustomerId() == null) {
            customerId = insertRow("INSERT INTO customers (email) VALUES (\"placeholder\");");
        } else {
            customerId = event.getCustomerId();
        }
        connectIpAddressToCustomer(customerId, ipAddressId);
        final String logEventTemplate = "INSERT INTO ad_events " +
                "(clientId, ipAddress, type, platform, adId, adsetId, campaignId, customerId) " +
                "VALUES ({0});";
        final String adEventValues =
                event.getClientId() + ","
                        + "\"" + event.getIpAddress() + "\","
                        + "\"click\","
                        + "\"facebook\","
                        + "\"" + event.getAdId() + "\","
                        + "\"" + event.getAdsetId() + "\","
                        + "\"" + event.getCampaignId() + "\","
                        + customerId;
        insertRow(MessageFormat.format(logEventTemplate, adEventValues));
        return customerId;
    }

    /**
     * Helper function for validating that a click event has all its required fields for processing.
     * @param eventPost the EventPost object to validate.
     * @throws MissingEventInfoException if any required fields are missing
     */
    void validateClickEventIsValid(
            @NonNull final EventPost eventPost) throws MissingEventInfoException {
        final List<String> missingRequiredFields = new ArrayList<>();
        if (eventPost.getClientId() == null) {
            missingRequiredFields.add("client id");
        }
        if (eventPost.getAdId() == null) {
            missingRequiredFields.add("ad id");
        }
        if (eventPost.getAdsetId() == null) {
            missingRequiredFields.add("adset id");
        }
        if (eventPost.getCampaignId() == null) {
            missingRequiredFields.add("campaign id");
        }
        if (eventPost.getIpAddress() == null) {
            missingRequiredFields.add("ip address");
        }
        if (!missingRequiredFields.isEmpty()) {
            final String missingFieldsTemplate = "This event is missing the following required fields: {0}";
            throw new MissingEventInfoException(
                    MessageFormat.format(
                            missingFieldsTemplate,
                            String.join(",", missingRequiredFields)));
        }
    }

    /**
     * Helper function for obtaining the id of an Ip Address from the database.
     * @param ipAddress String representing the IP Address to register
     * @return an integer representing the id of the IP Address in the Phoenix DB.
     * @throws SQLException
     */
    int registerIpAddress(@NonNull final String ipAddress) throws SQLException {
        final String ipAddressQuery = "SELECT * FROM ip_addresses WHERE address = \"{0}\";";
        final String ipAddressInsert = "INSERT INTO ip_addresses (address) VALUES (\"{0}\");";
        final ResultSet ipAddressRows = pullRows(MessageFormat.format(ipAddressQuery, ipAddress));
        if (ipAddressRows.next()) {
            return Integer.valueOf(ipAddressRows.getString("ipAddressId"));
        } else {
            return insertRow(MessageFormat.format(ipAddressInsert, ipAddress));
        }
    }

    /**
     * Helper function for obtaining the id of the connection between a user and an ip address.
     * If the user and ip address have not been connected, a connection is created between them.
     * @param customerId the Customer Id to check for a connection with.
     * @param addressId the Ip Address Id the check for a connection with.
     * @return the connection Id.
     * @throws SQLException
     */
    int connectIpAddressToCustomer(
            @NonNull final long customerId,
            @NonNull final long addressId) throws SQLException {
        final String statementTemplate =
                "SELECT * FROM customer_to_address WHERE customerId = {0} AND addressId = {1};";
        final ResultSet resultSet = pullRows(
                MessageFormat.format(statementTemplate, customerId, addressId));
        if (resultSet.next()) {
            return Integer.valueOf(resultSet.getString("connectionId"));
        } else {
            final String insertTemplate =
                    "INSERT INTO customer_to_address (customerId, addressId) VALUES ({0}, {1});";
            return insertRow(
                    MessageFormat.format(insertTemplate, customerId, addressId));
        }
    }

    /**
     * Helper function for running insert queries and returning the id
     * @param statement the statement to run to insert new data into the database.
     * @return the id number for the newly inserted row.
     * @throws SQLException if there is an issue with executing the SQL query.
     */
    int insertRow(
            @NonNull final String statement) throws SQLException{
        final PreparedStatement sqlStatement =
                phoenixConn.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
        sqlStatement.executeUpdate();
        final ResultSet resultSet = sqlStatement.getGeneratedKeys();
        resultSet.next();
        return resultSet.getInt(1);
    }

    /**
     * Helper function for obtaining rows from a table based on some query.
     * @param query the query to run to pull data from the database.
     * @return ResultSet of the query.
     * @throws SQLException if there is an issue with executing the SQL query.
     */
    ResultSet pullRows(@NonNull final String query) throws SQLException{
        final PreparedStatement statement = phoenixConn.prepareStatement(query);
        final ResultSet resultSet = statement.executeQuery();
        return resultSet;
    }

    /**
     * Function for executing removal statements.
     * @param statement SQL statement for removing rows.
     * @throws SQLException
     */
    void removeRow(@NonNull final String statement) throws SQLException{
        final PreparedStatement sqlStatement = phoenixConn.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
        sqlStatement.execute(statement);
    }

}
