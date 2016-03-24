package edu.umich.verdict.cli;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.CannotConnectException;
import edu.umich.verdict.connectors.DbConnector;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;

import edu.umich.verdict.connectors.DbmsNotSupportedException;
import jline.console.ConsoleReader;
import jline.console.history.*;
import org.apache.commons.cli.*;

public class Cli {
    public static void main(String[] args) {
        Configuration conf;
        try {
            conf = getConfig(args);
        } catch (FileNotFoundException e) {
            System.err.print(e.getMessage());
            return;
        }
        if (conf == null) {
            System.err.println("Wrong arguments; You should either specify a DBMS to connect to (e.g. '-dbms " +
                    "impala') or provide a config file (e.g. '-conf path/to/file.config')");
            return;
        }
        try {
            new Cli(conf).run();
        } catch (DbmsNotSupportedException e) {
            System.err.println(e.getMessage());
        } catch (CannotConnectException e) {
            System.err.println(e.getMessage());
            System.err.println(e.getCause().getMessage());
        } catch (IOException e) {
            System.err.println("Error occurred while initializing CLI.");
            System.err.println(e.getMessage());
        }
    }

    private static Configuration getConfig(String[] args) throws FileNotFoundException {
        Options options = new Options();
        options.addOption(Option.builder("dbms")
                .hasArg()
                .longOpt("dbms")
                .desc("The DBMS on top of which Verdict runs (Impala, Hive, etc.)")
                .build());

        options.addOption(Option.builder("conf")
                .hasArg()
                .longOpt("conf")
                .desc("Path to config file")
                .build());

        try {
            org.apache.commons.cli.CommandLine commandLine = new DefaultParser().parse(options, args);
            String dbms = commandLine.getOptionValue("dbms", null), file = commandLine.getOptionValue("conf", null);
            if(dbms == null && file==null)
                return null;
            Configuration conf = file == null ? new Configuration() : new Configuration(new File(file));
            if (dbms != null)
                conf.set("dbms", dbms);
            return conf;
        } catch (ParseException e) {
            return null;
        }
    }

    private DbConnector connector;
    private Configuration config;
    private ConsoleReader reader;
    private PrintWriter out;

    public Cli(Configuration config) throws DbmsNotSupportedException, CannotConnectException, IOException {
        this.config = config;
        this.connector = DbConnector.createConnector(config);
        System.out.println("Successfully connected to " + config.get("dbms") + ".");
        System.out.println(connector.getMetaDataManager().getSamplesCount() + " registered sample(s) found.");

        setShutDown();
        setReader();
    }

    private void setShutDown() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println();
                System.out.println("Closing database connections...");
                connector.close();
                System.out.println("Goodbye!");
            } catch (SQLException e) {
                System.err.println("Error while trying to close db connections: ");
                e.printStackTrace();
            }
        }));
    }

    private void setReader() throws IOException {
        reader = new ConsoleReader();
        out = new PrintWriter(reader.getOutput());
        reader.setExpandEvents(false);
        reader.setBellEnabled(false);
        setupHistory();
    }

    private void setupHistory() {
        final String HISTORYFILE = System.getProperty("user.home") + File.separator + ".verdict-history";
        try {
            PersistentHistory history = new FileHistory(new File(HISTORYFILE));
            reader.setHistory(history);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    ((FileHistory) reader.getHistory()).flush();
                } catch (IOException e) {
                    System.err.println("WARNING: Failed to save command history:");
                    System.err.println(e.getMessage());
                }
            }));
        } catch (Exception e) {
            System.err.println("WARNING: Error while initializing Verdict's history:");
            System.err.println(e.getMessage());
        }
    }

    private void run() {
        while (true) {
            out.flush();
            String str = getNewQuery();
            if (str == null)
                break;
            RunningResults rr = new RunningResults(str);
            rr.run(config, connector);
            rr.printResults(out);
        }
        System.exit(0);
    }

    private String getNewQuery() {
        String PROMPT = "verdict> ";
        String q = "", l = "";
        History h = reader.getHistory();
        int hSize = h.size();
        String multilinePrompt = "       > ";
        while (!l.trim().endsWith(";")) {
            try {
                l = reader.readLine(q.isEmpty() ? PROMPT : multilinePrompt, null);
            } catch (IOException e) {
                System.err.print("jLine error:");
                e.printStackTrace();
                return null;
            }
            if (l == null)
                return null;
            l = l.trim();
            if (!l.isEmpty())
                q += l + " ";
            if (q.equals("\\q "))
                return null;
        }
        q = q.trim();
        while (hSize < h.size())
            h.removeLast();
        h.add(q);
        return q;
    }
}
