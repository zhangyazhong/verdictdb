package edu.umich.verdict;

import java.io.*;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;

public class Configuration {
    private HashMap<String, String> configs = new HashMap<>();

    public Configuration() {
        setDefaults();
    }

    public Configuration(File file) throws FileNotFoundException {
        this();
        updateFromStream(new FileInputStream(file));
    }

    private Configuration setDefaults() {
        try {
            ClassLoader cl = this.getClass().getClassLoader();
            updateFromStream(cl.getResourceAsStream("default.conf"));
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
        }
        return this;
    }

    private Configuration updateFromStream(InputStream stream) throws FileNotFoundException {
        Scanner scanner = new Scanner(stream);
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            if (!line.isEmpty() && !line.startsWith("#"))
                set(line);
        }
        return this;
    }

    public int getInt(String key) {
        return Integer.parseInt(get(key));
    }

    public boolean getBoolean(String key) {
        String val = get(key).toLowerCase();
        return val.equals("on") || val.equals("yes") || val.equals("true") || val.equals("1");
    }

    public double getDouble(String key) {
        return Double.parseDouble(get(key));
    }

    public double getPercent(String key) {
        String val = get(key);
        if (val.endsWith("%"))
            return Double.parseDouble(val.substring(0, val.length() - 1)) / 100;
        return Double.parseDouble(val);
    }

    public String get(String key) {
        return configs.getOrDefault(key.toLowerCase(), configs.getOrDefault(configs.getOrDefault("dbms", "") + "." + key
                .toLowerCase(), null));
    }

    private Configuration set(String keyVal) {
        String[] parts = keyVal.split("=");
        String key = parts[0].trim();
        String val = parts[1].trim();
        if (val.startsWith("\"") && val.endsWith("\""))
            val = val.substring(1, val.length() - 1);
        return set(key, val);
    }

    // TODO: Key and value validation
    public Configuration set(String key, String value) {
        configs.put(key.toLowerCase(), value);
        return this;
    }
}
