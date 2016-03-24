package edu.umich.verdict.connectors;

public class CannotConnectException extends Exception {
    private final String dbms;
    private final Throwable cause;

    public CannotConnectException(String dbms, Throwable cause) {
        this.cause = cause;
        this.dbms = dbms;
    }

    public String getMessage() {
        return "Unable to connect to " + dbms + ".";
    }

    public Throwable getCause() {
        return cause;
    }
}
