package com.example.phoenix.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;

/**
 * POJO for User info.
 */
@Data
@AllArgsConstructor
@Generated
public class User {

    /**
     * The id of the user.
     */
    private int id;

    /**
     * The first name of the user.
     */
    private String firstname;

    /**
     * The last name of the user.
     */
    private String lastname;

    /**
     * The username the user will use to log in.
     */
    private String username;

    /**
     * The password the user will use to log in.
     */
    private String password;
}
