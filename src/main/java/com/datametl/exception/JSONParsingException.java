package com.datametl.exception;

/**
 * Created by TseAndy on 3/1/17.
 */
public class JSONParsingException extends Exception{

    public JSONParsingException() {

    }

    public JSONParsingException(String message) {

        super(message);
    }

    public JSONParsingException(String message, Throwable cause) {
        super(message, cause);
    }

    public JSONParsingException(Throwable cause) {
        super(cause);
    }
}
