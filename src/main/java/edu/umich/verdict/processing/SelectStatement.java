package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.transformation.QueryTransformer;
import edu.umich.verdict.transformation.TransformedQuery;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SelectStatement extends ParsedStatement {
    public SelectStatement(String str, ParseTree tree, TokenStreamRewriter rewriter) {
        super(str, tree, rewriter);
    }

    @Override
    public ResultSet run(Configuration conf, DbConnector connector) throws SQLException {
        ResultSet rs;
        TransformedQuery transformed = QueryTransformer.forConfig(conf, connector.getMetaDataManager(), this).transform();
        String q = transformed.toString();
        if (transformed.isChanged()) {
            info("New Query:");
            info(q);
            info("\n");
            info("Using Sample: " + transformed.getSample().name + " Size: " + (transformed.getSample().compRatio *
                    100) + "%" + " Type: " + (transformed.getSample().stratified ? "Stratified" : "Uniform"));
            info("Bootstrap Trials: " + transformed.getBootstrapRepeats());
            info("Method: " + transformed.getMethod());
            rs = connector.executeQuery(q);
            //TODO: do we need this?
//                if (!transformer.isUseConfIntUdf())
//                    rs = new ResultSetWrapper(rs, transformed);
        } else {
            info("Running the original query...");
            rs = connector.executeQuery(q);
        }
        return rs;
    }
}
