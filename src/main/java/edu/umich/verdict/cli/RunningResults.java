package edu.umich.verdict.cli;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.processing.ParsedStatement;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RunningResults {
    public boolean failed = false;
    public Exception ex;
    public ResultSet rs;
    public long latency;
    public ParsedStatement q;

    public RunningResults(String str) {
        try {
            this.q = ParsedStatement.fromString(str);
        } catch (Exception e) {
            this.ex = e;
            this.failed = true;
        }
    }

    public void run(Configuration config, DbConnector connector) {
        if (failed)
            return;
        try {
            long s = System.currentTimeMillis();
            rs = q.run(config, connector);
            latency = System.currentTimeMillis() - s;
        } catch (Exception e) {
            this.ex = e;
            this.failed = true;
        }
    }

    public void printResults() {
        if (failed) {
            if (ex instanceof SQLException)
                System.err.println(ex.getMessage());
            else
                ex.printStackTrace();
            return;
        }

        //TODO: don't always print it. use logger
        System.out.println("(" + latency + " ms)");
        System.out.println();

        if (rs != null) {
            try {
                ResultWriter.writeResultSet(System.out, rs);
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            }
        } else {
            System.out.println("Done.");
        }
    }
}
