package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.parser.TsqlParser;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashSet;

public class ConfigStatement extends VerdictStatement {
    private static final HashSet validKeys = new HashSet<>(Arrays.asList(
            "approximation",
            "bootstrap.method",
            "bootstrap.trials",
            "confidence",
            "sample_size",
            "sample_type",
            "error_columns",
            "fixed_sample"
    ));

    private final String key;
    private String value;

    public ConfigStatement(String str, ParseTree tree) {
        super(str, tree);
        ParseTree ctx = tree.getChild(0);
        if (ctx instanceof TsqlParser.Config_set_statementContext) {
            key = ((TsqlParser.Config_set_statementContext) ctx).key.getText();
            value = ((TsqlParser.Config_set_statementContext) ctx).value.getText() + (((TsqlParser.Config_set_statementContext) ctx).percent != null ? "%" : "");
            if (value.startsWith("\"") && value.endsWith("\""))
                value = value.substring(1, value.length() - 1);
        } else {
            key = ((TsqlParser.Config_get_statementContext) ctx).key.getText();
            value = null;
        }
    }

    @Override
    public ResultSet run(Configuration conf, DbConnector connector) throws InvalidConfigurationException {
        if (!isValidKey(key))
            throw new InvalidConfigurationException("'" + key + "' is not a valid option.");
        if (value == null) {
            String val = conf.get(key);
            info(key + ": " + (val == null ? "NULL" : val));
        } else {
            conf.set(key, value);
        }
        return null;
    }

    private boolean isValidKey(String key) {
        return validKeys.contains(key);
    }
}
