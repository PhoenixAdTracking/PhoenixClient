package com.example.phoenix.ingestion;

import com.example.phoenix.models.User;
import lombok.NonNull;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Class used to organize all data ingestion business logic.
 */
public class PhoenixDataProcessor {

    private static final String USER_ID_COLUMN = "userId";

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
        final String query =
                "INSERT INTO users (username, password, firstname, lastname)"
                        + "VALUES (\"" + user.getUsername() + "\", \"" + user.getPassword() + "\", \"" + user.getFirstname() + "\", \"" + user.getLastname() + "\");";
        return insertRow(query, USER_ID_COLUMN);
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
}
