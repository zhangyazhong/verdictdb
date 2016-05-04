package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.parser.TsqlParser;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DeleteSampleStatement extends VerdictStatement {
    private final String sample;

    public DeleteSampleStatement(String str, ParseTree tree) {
        super(str, tree);
        this.sample = ((TsqlParser.Delete_sample_statementContext) tree)
                .table_name().getText();
    }

    @Override
    public ResultSet run(Configuration conf, DbConnector connector) throws SQLException {
        connector.getMetaDataManager().deleteSample(sample);
        return null;
    }
}
