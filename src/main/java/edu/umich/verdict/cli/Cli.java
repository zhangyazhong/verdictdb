package edu.umich.verdict.cli;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Scanner;

public class Cli {

    private DbConnector connector;
    private Configuration config;
    private Scanner sc = new Scanner(System.in);

    public Cli(Configuration config) throws Exception {
        this.config = config;
        this.connector = DbConnector.createConnector(config);
        System.out.println("Successfully connected to " + config.get("dbms") + ".");
        System.out.println(connector.getMetaDataManager().getSamplesCount() + " registered sample(s) found;");
    }

    public static void main(String[] args) {
        Configuration conf;
        try {
            conf = getConfig(args);
        } catch (FileNotFoundException e) {
            System.err.print(e.getMessage());
            return;
        }
        if (conf == null) {
            System.err.println("Wrong argument, You should either specify a DBMS to connect to (e.g. '-dbms  " +
                    "impala') or provide a config file (e.g. '-conf path/to/file.config'");
            return;
        }
        try {
            new Cli(conf).run();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    private static Configuration getConfig(String[] args) throws FileNotFoundException {
        if (args.length != 2 && args.length != 4)
            return null;
        String file = null, dbms = null;
        switch (args[0]) {
            case "-dbms":
                dbms = args[1];
                break;
            case "-conf":
                file = args[1];
                break;
            default:
                return null;
        }
        if (args.length == 4) {
            switch (args[2]) {
                case "-dbms":
                    dbms = args[3];
                    break;
                case "-conf":
                    file = args[3];
                    break;
                default:
                    return null;
            }
        }
        Configuration conf = file == null ? new Configuration() : new Configuration(new File(file));
        if (dbms != null)
            conf.set("dbms", dbms);
        return conf;
    }

    private void run() {
        while (true) {
            System.out.flush();
            System.err.flush();
            System.out.print("\n> ");
            String str = getNewQuery();
            if(str==null)
                break;
            RunningResults rr = new RunningResults(str);
            rr.run(config, connector);
            rr.printResults();
        }
        System.out.println("Goodbye!");
        try {
            connector.close();
        } catch (SQLException e) {
            System.err.println("Error while trying to close db connections: ");
            e.printStackTrace();
        }
    }

    private String getNewQuery() {
        String q = "", l = "";
        while (!l.trim().endsWith(";")) {
            l = sc.nextLine();
            q += l + " ";
            if(q.equals("\\q "))
                return null;
        }
        return q;
    }
}
