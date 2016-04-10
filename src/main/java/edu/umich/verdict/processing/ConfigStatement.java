package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.parser.TsqlParser;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;

public class ConfigStatement extends ParsedStatement {
    private final String key;
    private final String value;

    public ConfigStatement(String str, ParseTree tree) {
        super(str, tree);
        ParseTree ctx = tree.getChild(0);
        if (ctx instanceof TsqlParser.Config_set_statementContext) {
            key = ((TsqlParser.Config_set_statementContext) ctx).key.getText();
            value = ((TsqlParser.Config_set_statementContext) ctx).value.getText() + (((TsqlParser.Config_set_statementContext) ctx).percent != null ? "%" : "");
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
        return key.startsWith("bootstrap.");
    }
}
