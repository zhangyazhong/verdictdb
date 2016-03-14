package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.transformation.QueryTransformer;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ShowSamplesStatement extends ParsedStatement {
    public ShowSamplesStatement(String str, ParseTree tree, TokenStreamRewriter rewriter) {
        super(str, tree, rewriter);
    }

    @Override
    public ResultSet run(Configuration conf, DbConnector connector) throws SQLException {
        return connector.getMetaDataManager().getSamplesInfo();
    }
}
