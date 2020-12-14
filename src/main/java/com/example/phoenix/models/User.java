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

    /**
     * NOTE: Only used as part of a Post request to tie a user to a business.
     * The id of the business that this user is attached to.
     */
    private int businessId;

    /**
     * NOTE: Only used as part of a Post request to tie a user to a business.
     * The role of the user for a business.
     */
    private String role;
}
