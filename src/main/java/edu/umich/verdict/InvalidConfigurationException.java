package edu.umich.verdict;

public class InvalidConfigurationException extends Exception {
    public InvalidConfigurationException(String message) {
        super("Invalid configuration:\n" + message);
    }
}
