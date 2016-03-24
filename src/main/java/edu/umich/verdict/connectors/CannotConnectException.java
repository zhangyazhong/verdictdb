package edu.umich.verdict.connectors;

public class CannotConnectException extends Exception {
    private final String dbms;
    private final Exception cause;

    public CannotConnectException(String dbms, Exception cause) {
        this.cause = cause;
        this.dbms = dbms;
    }

    public String getMessage() {
        return "Unable to connect to " + dbms + ".";
    }

    public Exception getCause() {
        return cause;
    }
}
