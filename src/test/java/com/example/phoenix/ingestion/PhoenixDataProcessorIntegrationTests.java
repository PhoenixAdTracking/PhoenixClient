package com.example.phoenix.ingestion;

import com.example.phoenix.DatabaseConfig;
import com.example.phoenix.models.User;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import javax.annotation.Resource;
import java.net.URI;
import java.sql.ResultSet;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = DatabaseConfig.class)
public class PhoenixDataProcessorIntegrationTests {

    @Autowired
    private BasicDataSource basicDataSource;


    @Test
    public void testWhenSearchingForTestUserInDatabaseThenTestUserIsFound() throws Exception{
        final String testFirstName = "testFirstName";
        final String testLastName = "testLastName";
        final String testUsername = "testUsername";
        final String testPassword = "testPassword";

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

}
