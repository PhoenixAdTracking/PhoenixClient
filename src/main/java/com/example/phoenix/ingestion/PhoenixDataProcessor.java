package com.example.phoenix.ingestion;

import com.example.phoenix.models.Business;
import com.example.phoenix.models.User;
import lombok.NonNull;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.security.core.userdetails.UserDetails;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

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

    public PhoenixDataProcessor (@NonNull final BasicDataSource phoenixDb) {
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
}
