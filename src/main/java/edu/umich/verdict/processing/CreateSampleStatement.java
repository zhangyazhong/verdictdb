package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.transformation.QueryTransformer;
import edu.umich.verdict.models.Sample;
import edu.umich.verdict.parser.HplsqlParser;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;

public class CreateSampleStatement extends ParsedStatement {
    private Sample sample;

    public CreateSampleStatement(String str, ParseTree tree, TokenStreamRewriter rewriter) {
        super(str, tree, rewriter);
        HplsqlParser.Create_sample_stmtContext t = (HplsqlParser.Create_sample_stmtContext) tree.getChild(0).getChild(0);
        String name = t.getChild(2).getText();
        String table = t.getChild(4).getText();
        boolean stratified = t.T_STRATIFIED() != null;
        String[] strataCols = new String[0];
        if (stratified) {
            strataCols = new String[t.expr().size()];
            for (int i = 0; i < strataCols.length; i++)
                strataCols[i] = t.expr(i).getText();
        }
        int poissonCols = t.T_POISSON() != null ? Integer.parseInt(t.children.get(9).getText()) : 0;
        double size = Double.parseDouble(t.getChild(6).getText()) / 100;
        sample = new Sample(name, table, size, 0, poissonCols, strataCols);
    }

    @Override
    public ResultSet run(Configuration conf, DbConnector connector) throws Exception {
        try {
            info("Creating sample...");
            connector.getMetaDataManager().createSample(sample);
            info("Sample created.");
        } catch (Exception e) {
            System.err.println("Error in creating sample: ");
            throw e;
        }
        return null;
    }
}
