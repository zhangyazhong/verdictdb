package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.transformation.QueryTransformer;
import edu.umich.verdict.parser.HplsqlParser;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DeleteSampleStatement extends ParsedStatement {
    private final String sample;

    public DeleteSampleStatement(String str, ParseTree tree, TokenStreamRewriter rewriter) {
        super(str, tree, rewriter);
        this.sample = ((HplsqlParser.Delete_sample_stmtContext) tree.getChild(0).getChild(0))
                .table_name().getText();
    }

    @Override
    public ResultSet run(Configuration conf, DbConnector connector) throws SQLException {
        connector.getMetaDataManager().deleteSample(sample);
        return null;
    }
}
