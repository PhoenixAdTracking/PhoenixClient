package com.example.phoenix;

import lombok.NonNull;

public class MissingEventInfoException extends Exception{
    public MissingEventInfoException (@NonNull final String message) {
        super(message);
    }
}
