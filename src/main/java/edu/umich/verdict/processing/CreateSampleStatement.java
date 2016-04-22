package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.models.StratifiedSample;
import edu.umich.verdict.parser.TsqlParser;
import edu.umich.verdict.models.Sample;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;

public class CreateSampleStatement extends ParsedStatement {
    private Sample sample;

    public CreateSampleStatement(String str, ParseTree tree) {
        super(str, tree);
        TsqlParser.Create_sample_statementContext ctx = (TsqlParser.Create_sample_statementContext) tree;
        String name = ctx.sample.getText();
        String table = ctx.table.getText();
        int poissonCols = ctx.POISSON() != null ? Integer.parseInt(ctx.poission_cols.getText()) : 0;
        double size = Double.parseDouble(ctx.size.getText()) / 100;
        boolean stratified = ctx.STRATIFIED() != null;
        if (stratified) {
            String[] strataCols = new String[ctx.column_name().size()];
            for (int i = 0; i < strataCols.length; i++)
                strataCols[i] = ctx.column_name(i).getText();
            sample = new StratifiedSample(name, table, size, 0, poissonCols, strataCols);
        } else
            sample = new Sample(name, table, size, 0, poissonCols);
    }

    @Override
    public ResultSet run(Configuration conf, DbConnector connector) throws Exception {
        try {
            info("Creating sample...");
            connector.getMetaDataManager().createSample(sample);
        } catch (Exception e) {
            System.err.println("Error while creating sample: ");
            throw e;
        }
        return null;
    }
}
