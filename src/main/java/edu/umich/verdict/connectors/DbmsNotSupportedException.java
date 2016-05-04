package edu.umich.verdict.connectors;

import edu.umich.verdict.VerdictException;

public class DbmsNotSupportedException extends VerdictException {
    private final ClassNotFoundException cause;
    private final String dbms;

    public DbmsNotSupportedException(String dbms, ClassNotFoundException cause) {
        this.cause = cause;
        this.dbms = dbms;
    }

    public String getMessage() {
        return dbms + " is not a supported DBMS.\n"+"Class "+cause.getMessage()+" was not found.";
    }

    public Exception getCause(){
        return cause;
    }
}
