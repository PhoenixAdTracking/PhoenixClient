package com.example.phoenix.ingestion;

import com.example.phoenix.PhoenixClient;
import com.example.phoenix.models.Business;
import com.example.phoenix.models.User;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.*;

public class PhoenixDataProcessorTests {

    private BasicDataSource mockDataSource;

    private Connection mockConnection;

    private PhoenixDataProcessor phoenixDataProcessor;

    private PreparedStatement mockStatement;

    private ResultSet mockResultSet;

    @BeforeEach
    public void setup() throws Exception{
        mockDataSource = Mockito.mock(BasicDataSource.class);
        mockConnection = Mockito.mock(Connection.class);
        mockStatement = Mockito.mock(PreparedStatement.class);
        mockResultSet = Mockito.mock(ResultSet.class);
        Mockito.when(mockDataSource.getConnection()).thenReturn(mockConnection);
        Mockito.when(mockConnection.prepareStatement(anyString(), anyInt())).thenReturn(mockStatement);
        Mockito.when(mockStatement.getGeneratedKeys()).thenReturn(mockResultSet);
        Mockito.when(mockResultSet.getInt(anyString())).thenReturn(-1);
        phoenixDataProcessor = new PhoenixDataProcessor(mockDataSource);
    }

    @Test
    public void testWhenCreateUserIsGivenUserObjectThenProperSqlQueryFormed() throws Exception {
        final String testFirstName = "testFirstName";
        final String testLastName = "testLastName";
        final String testUsername = "testUsername";
        final String testPassword = "testPassword";

        final User testUser = new User(
                -1,
                testFirstName,
                testLastName,
                testUsername,
                testPassword);
        phoenixDataProcessor.createUser(testUser);
        Mockito.verify(mockConnection).prepareStatement(
                "INSERT INTO users (username, password, firstname, lastname)"
                + " VALUES (\"" + testUsername + "\", \"" + testPassword + "\", \"" + testFirstName + "\", \"" + testLastName + "\");",
                Statement.RETURN_GENERATED_KEYS);
    }

    @Test
    public void testWhenCreateBusinessIsGivenBusinessObjectThenProperSqlQueryFormed() throws Exception {
        final String testName = "testName";
        final Business testBusiness = new Business(-1, testName);
        phoenixDataProcessor.createBusiness(testBusiness);
        Mockito.verify(mockConnection).prepareStatement(
                "INSERT INTO businesses (name) VALUES (\"" + testName + "\");",
                Statement.RETURN_GENERATED_KEYS);
    }
}
