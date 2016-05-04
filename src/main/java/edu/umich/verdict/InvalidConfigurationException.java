package edu.umich.verdict;

public class InvalidConfigurationException extends VerdictException {
    public InvalidConfigurationException(String message) {
        super("Invalid configuration:\n" + message);
    }
}
