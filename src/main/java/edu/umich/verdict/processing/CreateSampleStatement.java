package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.parser.TsqlParser;
import edu.umich.verdict.models.Sample;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;

public class CreateSampleStatement extends ParsedStatement {
    private Sample sample;

    public CreateSampleStatement(String str, ParseTree tree, TokenStreamRewriter rewriter) {
        super(str, tree, rewriter);
        TsqlParser.Create_sample_statementContext ctx = (TsqlParser.Create_sample_statementContext) tree;
        String name = ctx.sample.getText();
        String table = ctx.table.getText();
        boolean stratified = ctx.T_STRATIFIED() != null;
        String[] strataCols = new String[0];
        if (stratified) {
            strataCols = new String[ctx.column_name().size()];
            for (int i = 0; i < strataCols.length; i++)
                strataCols[i] = ctx.column_name(i).getText();
        }
        int poissonCols = ctx.T_POISSON() != null ? Integer.parseInt(ctx.poission_cols.getText()) : 0;
        double size = Double.parseDouble(ctx.size.getText()) / 100;
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
