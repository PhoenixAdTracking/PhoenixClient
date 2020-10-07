package com.example.phoenix.ingestion;

import com.example.phoenix.DatabaseConfig;
import com.example.phoenix.models.InsightType;
import com.example.phoenix.models.Insights;
import com.google.common.collect.ImmutableList;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.sql.ResultSet;
import java.util.List;

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

        final ResultSet resultSet = basicDataSource.getConnection().prepareStatement("SELECT * FROM businesses WHERE businessId = -1").executeQuery();
        resultSet.next();
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

}
