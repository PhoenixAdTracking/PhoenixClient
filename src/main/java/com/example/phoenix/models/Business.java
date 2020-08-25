package com.example.phoenix.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;

/**
 * POJO model class for a business's information.
 */
@Data
@Generated
@AllArgsConstructor
public class Business {
    /**
     * The id of the business.
     */
    private int id;

    /**
     * The name of the business.
     */
    private String name;
}
