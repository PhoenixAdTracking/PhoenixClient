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
        phoenixDataProcessor = new PhoenixDataProcessor(mockConnection);
    }

//    @Test
//    public void testWhenCreateUserIsGivenUserObjectThenProperSqlQueryFormed() throws Exception {
//        final User testUser = new User(
//                -1,
//                "testFirstName",
//                "testLastName",
//                "testUsername",
//                "password",
//                -1,
//                "admin");
//        phoenixDataProcessor.createUser(testUser);
//        Mockito.verify(mockConnection).prepareStatement(
//                "INSERT INTO users (username, password, firstname, lastname)"
//                        + " VALUES (\"" + testUser.getUsername() + "\", \"" + testUser.getPassword() + "\", \"" + testUser.getFirstname() + "\", \"" + testUser.getLastname() + "\");",
//                Statement.RETURN_GENERATED_KEYS);
//        Mockito.verify(mockConnection).prepareStatement(
//                "INSERT INTO user_to_business (userId, businessId, active, role)"
//                        + " VALUES (-1, -1, 1, \"" + testUser.getRole() + "\");",
//                Statement.RETURN_GENERATED_KEYS);
//
//    }

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
