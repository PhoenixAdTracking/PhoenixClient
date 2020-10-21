package com.example.phoenix.ingestion;

import com.example.phoenix.DatabaseConfig;
import com.example.phoenix.models.EventPost;
import com.example.phoenix.models.EventType;
import com.example.phoenix.models.InsightType;
import com.example.phoenix.models.Insights;
import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = DatabaseConfig.class)
public class PhoenixDataProcessorIntegrationTests {

    @Autowired
    private BasicDataSource basicDataSource;

    @Autowired
    private PhoenixDataProcessor dataProcessor;

    @Test
    public void testWhenSearchingForTestUserInDatabaseThenTestUserIsFound() throws Exception{
        final String testFirstName = "testFirstName";
        final String testLastName = "testLastName";
        final String testUsername = "testUsername";
        final String testPassword = "$2a$10$HgzJ1NQwYD1cGwvGTpd28.9vhPh3kL015mnBFhGE/bLnei0q6hTmi";

        final ResultSet resultSet = basicDataSource.getConnection().prepareStatement("SELECT * FROM users WHERE firstname = \"testFirstName\"").executeQuery();
        resultSet.next();
        Assert.assertEquals(testFirstName, resultSet.getString("firstname"));
        Assert.assertEquals(testLastName, resultSet.getString("lastname"));
        Assert.assertEquals(testUsername, resultSet.getString("username"));
        Assert.assertEquals(testPassword, resultSet.getString("password"));
    }

    @Test
    public void testWhenSearchingForTestBusinessInDatabaseThenTestBusinessIsFound() throws Exception{
        final String testName = "testBusiness";

        final ResultSet resultSet = basicDataSource.getConnection().prepareStatement("SELECT * FROM businesses WHERE businessId = 1").executeQuery();
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(testName, resultSet.getString("name"));
    }

    @Test
    public void testWhenGivenCampaignInsightsThenAddPhoenixMetricsAddsCampaignMetrics() {
        final List<Insights> testInsights = ImmutableList.of(Insights.builder().id("1").type(InsightType.CAMPAIGN).build());
        final List<Insights> resultInsights = dataProcessor.addPhoenixMetrics(testInsights);
        Assert.assertFalse(resultInsights.isEmpty());
        final Insights insightWithPhoenixPurchases = resultInsights.get(0);
        Assert.assertEquals(1, insightWithPhoenixPurchases.getPhoenixPurchases());
        Assert.assertEquals(1.00, insightWithPhoenixPurchases.getTotalSales(), 0.00);
    }

    @Test
    public void testWhenGivenAdSetInsightsThenAddPhoenixMetricsAddsCampaignMetrics() {
        final List<Insights> testInsights = ImmutableList.of(Insights.builder().id("1").type(InsightType.AD_SET).build());
        final List<Insights> resultInsights = dataProcessor.addPhoenixMetrics(testInsights);
        Assert.assertFalse(resultInsights.isEmpty());
        final Insights insightWithPhoenixPurchases = resultInsights.get(0);
        Assert.assertEquals(1, insightWithPhoenixPurchases.getPhoenixPurchases());
        Assert.assertEquals(1.00, insightWithPhoenixPurchases.getTotalSales(), 0.00);
    }

    @Test
    public void testWhenGivenAdInsightsThenAddPhoenixMetricsAddsCampaignMetrics() {
        final List<Insights> testInsights = ImmutableList.of(Insights.builder().id("1").type(InsightType.AD).build());
        final List<Insights> resultInsights = dataProcessor.addPhoenixMetrics(testInsights);
        Assert.assertFalse(resultInsights.isEmpty());
        final Insights insightWithPhoenixPurchases = resultInsights.get(0);
        Assert.assertEquals(1, insightWithPhoenixPurchases.getPhoenixPurchases());
        Assert.assertEquals(1.00, insightWithPhoenixPurchases.getTotalSales(), 0.00);
    }

    @Test
    public void testWhenGivenExistingIpAddressThenGetIpAddressIdReturnsExistingId() throws Exception {
        Assert.assertEquals(1, dataProcessor.registerIpAddress("1"), 0);
    }

    @Test
    public void testWhenGivenNewIpAddressThenGetIpAddressIdReturnsNewId() throws Exception{
            final String testIpAddress = "TestIpAddress";
            dataProcessor.removeRow(MessageFormat.format(
                    "DELETE FROM ip_addresses WHERE address = \"{0}\";",
                    testIpAddress));
            final int newIpAddressId = dataProcessor.registerIpAddress(testIpAddress);
            Assert.assertNotEquals(-1, newIpAddressId);
            dataProcessor.removeRow(MessageFormat.format(
                    "DELETE FROM ip_addresses WHERE address = \"{0}\";",
                    testIpAddress));
    }

    @Test
    public void testWhenGivenEventPostWithExistingUserAndIpAddressThenEventIsInsertedIntoDatabaseAndCustomerIdIsReturned() throws Exception {
        final long testCustomerId = 1;
        final String testIpAddress = "TestIpAddress";
        final long testClientId = 1;
        final String testAdId = "testAdId";
        final String testAdsetId = "testAdsetId";
        final String testCampaignId = "testCampaignId";
        final String pullTestRowsQuery = "SELECT * FROM ad_events WHERE customerId = {0};";
        final String deleteTestRowsStatement = "DELETE FROM ad_events WHERE customerId = {0};";
        dataProcessor.removeRow(MessageFormat.format(deleteTestRowsStatement, testCustomerId));
        final ResultSet preInsertRows = dataProcessor.pullRows(MessageFormat.format(pullTestRowsQuery, testCustomerId));
        Assert.assertFalse(preInsertRows.next());
        final EventPost testEvent = EventPost.builder()
                .customerId(testCustomerId)
                .ipAddress(testIpAddress)
                .clientId(testClientId)
                .adId(testAdId)
                .adsetId(testAdsetId)
                .campaignId(testCampaignId)
                .build();
        dataProcessor.processClickEvent(testEvent);
        final ResultSet postInsertRows = dataProcessor.pullRows(MessageFormat.format(pullTestRowsQuery, testCustomerId));
        Assert.assertTrue(postInsertRows.next());
        dataProcessor.removeRow(MessageFormat.format(deleteTestRowsStatement, testCustomerId));
    }

    @Test
    public void testWhenGivenEventPostWithNoUserAndNewIpAddressThenEventIsInsertedIntoDatabaseAndCustomerIdIsReturned() throws Exception {
        final String testIpAddress = "TestIpAddress2";

        // Prep the database by removing any test info from it.
        final String testAddressRowsQuery = "SELECT * FROM ip_addresses WHERE address = \"{0}\";";
        final String testConnectionRowsQuery = "SELECT * FROM customer_to_address WHERE addressId = {0};";
        final String testCustomerRowsQuery = "SELECT * FROM customers WHERE customerId = {0};";
        final String testCustomerRowsDeleteStatement = "DELETE FROM customers WHERE customerId IN ({0});";
        final String testConnectionRowsDeleteStatement = "DELETE FROM customer_to_address WHERE connectionId IN ({0});";
        final String testAddressRowsDeleteStatement = "DELETE FROM ip_addresses WHERE address = \"{0}\";";
        final ResultSet testPreInsertAddressRows = dataProcessor.pullRows(MessageFormat.format(testAddressRowsQuery, testIpAddress));
        if (testPreInsertAddressRows.next()) {
            final int testIpAddressId = testPreInsertAddressRows.getInt(1);
            final ResultSet testConnectionRows = dataProcessor.pullRows(MessageFormat.format(testConnectionRowsQuery, testIpAddressId));
            final List<String> testConnectionIds = new ArrayList<>();
            final List<String> testCustomerIds = new ArrayList<>();
            while (testConnectionRows.next()) {
                testConnectionIds.add(String.valueOf(testConnectionRows.getInt(1)));
                testCustomerIds.add(String.valueOf(testConnectionRows.getInt("customerId")));
            }
            if (!testConnectionIds.isEmpty()) {
                final String testConnectionIdsString = String.join(",", testConnectionIds);
                final String testCustomerIdsString = String.join(",", testCustomerIds);
                dataProcessor.removeRow(MessageFormat.format(testConnectionRowsDeleteStatement, testConnectionIdsString));
                dataProcessor.removeRow(MessageFormat.format(testCustomerRowsDeleteStatement, testCustomerIdsString));
            }
            dataProcessor.removeRow(MessageFormat.format(testAddressRowsDeleteStatement, testIpAddress));
        }

        // Test code to insert event
        final long testClientId = 1;
        final String testAdId = "testAdId";
        final String testAdsetId = "testAdsetId";
        final String testCampaignId = "testCampaignId";
        final String pullTestRowsQuery = "SELECT * FROM ad_events WHERE customerId = {0};";
        final String deleteTestAdEventRowsStatement = "DELETE FROM ad_events WHERE customerId = {0};";
        final EventPost testEvent = EventPost.builder()
                .ipAddress(testIpAddress)
                .clientId(testClientId)
                .adId(testAdId)
                .adsetId(testAdsetId)
                .campaignId(testCampaignId)
                .build();
        final long testCustomerId = dataProcessor.processClickEvent(testEvent);

        // Check for all expected rows to be present
        // Check for test IP Address row
        final ResultSet testPostInsertIpAddress = dataProcessor.pullRows(MessageFormat.format(testAddressRowsQuery, testIpAddress));
        Assert.assertTrue(testPostInsertIpAddress.next());
        // Check for Address-to-Customer Connection rows connected to the test IP Address
        final int testPostInsertIpAddressId = testPostInsertIpAddress.getInt(1);
        final ResultSet testPostInsertConnectionRows = dataProcessor.pullRows(MessageFormat.format(testConnectionRowsQuery, testPostInsertIpAddressId));
        Assert.assertTrue(testPostInsertConnectionRows.next());
        // Check for a new customer row connected to the test IP Address
        final int testPostInsertCustomerId = testPostInsertConnectionRows.getInt("customerId");
        Assert.assertEquals(testCustomerId, testPostInsertCustomerId, 0);
        final ResultSet testPostInsertCustomerRows = dataProcessor.pullRows(MessageFormat.format(testCustomerRowsQuery, testCustomerId));
        Assert.assertTrue(testPostInsertCustomerRows.next());
        // Check that there is a new event attached to the newly created Customer
        final ResultSet testPostInsertEvent = dataProcessor.pullRows(MessageFormat.format(pullTestRowsQuery, testCustomerId));
        Assert.assertTrue(testPostInsertEvent.next());

        // Remove test rows
        dataProcessor.removeRow(MessageFormat.format(deleteTestAdEventRowsStatement, testCustomerId));
        dataProcessor.removeRow(MessageFormat.format(testConnectionRowsDeleteStatement, testPostInsertConnectionRows.getInt(1)));
        dataProcessor.removeRow(MessageFormat.format(testCustomerRowsDeleteStatement, testCustomerId));
        dataProcessor.removeRow(MessageFormat.format(testAddressRowsDeleteStatement, testIpAddress));

    }

    /**
     * Testing helper function for pulling the primary key.
     * @param resultSet the resultSet to try to pull a primary key for.
     * @return an Optional Integer that either contains the primary key of the result set or is empty.
     * @throws SQLException
     */
    private Optional<Integer> getRowId (@NonNull final ResultSet resultSet) throws SQLException {
        if (resultSet.next()) {
            return Optional.of(resultSet.getInt(0));
        } else {
            return Optional.empty();
        }
    }
}
