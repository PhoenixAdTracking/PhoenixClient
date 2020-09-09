package com.example.phoenix.ingestion;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ExternalDataFetcherIntegrationTest {

    /**
     * Note, this token expires 2 months after September 2nd, 2020.
     */
    final static private String FB_ACCESS_TOKEN = System.getenv("FACEBOOK_TEST_ACCESS_TOKEN");

    /**
     * Note, this token expires 2 months after September 2nd, 2020.
     */
    final static private String FB_TEST_USER_ID = System.getenv("FACEBOOK_TEST_USER_ID");

    private ExternalDataFetcher externalDataFetcher = new ExternalDataFetcher();

    @Test
    public void whenGivenUserIdAccessTokenThenGetFacebookAdAccountsReturnsListOfAdAccounts() throws Exception{
        final Map<String, String> testMap = externalDataFetcher.getFacebookAdAccounts(FB_ACCESS_TOKEN, FB_TEST_USER_ID);
        Assert.assertFalse(testMap.isEmpty());
        Assert.assertTrue(testMap.containsKey("Genesis Allure LLC 2"));
    }
}
