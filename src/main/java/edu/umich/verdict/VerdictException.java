package edu.umich.verdict;

import java.sql.SQLException;

public class VerdictException extends Exception {
    public VerdictException(String message){
        super(message);
    }

    public VerdictException() {
    }

    public SQLException toSQLException(){
        return new SQLException(this.getMessage(), this);
    }
}
