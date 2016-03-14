package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.transformation.QueryTransformer;
import edu.umich.verdict.parser.HplsqlParser;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;

public class SetConfigStatement extends ParsedStatement {
    public SetConfigStatement(String str, ParseTree tree, TokenStreamRewriter rewriter) {
        super(str, tree, rewriter);
        this.tree = tree.getChild(0).getChild(0);
    }

    //TODO: Make independent of parser
    @Override
    public ResultSet run(Configuration conf, DbConnector connector) {
        HplsqlParser.Set_configContext set = (HplsqlParser.Set_configContext) tree;
        if (set.set_bootstrap_trials() != null) {
            conf.set("bootstrap.trials", set.set_bootstrap_trials().int_number().getText());
            info("BOOTSTRAP TRIALS = " + conf.get("bootstrap.trials"));
        } else if (set.set_confidence() != null) {
            conf.set("bootstrap.confidence", (set.set_confidence().dec_number() != null ? set.set_confidence().dec_number().getText() : set.set_confidence().int_number().getText()) + "%");
            info("CONFIDENCE = " + conf.get("bootstrap.confidence"));
        } else if (set.set_use_samples() != null) {
            conf.set("bootstrap",set.set_use_samples().getChild(3).getText().toLowerCase());
            info(conf.getBoolean("bootstrap") ? "Using samples when available." : "Using original tables.");
        } else if (set.set_preferred_sample() != null) {
            conf.set("bootstrap.sample-size", (set.set_preferred_sample().children.size() == 6 ? set.set_preferred_sample().getChild(4).getText() : set.set_preferred_sample().getChild(3).getText())+"%");
            info("PREFERRED SAMPLE SIZE: " + conf.get("bootstrap.sample-size"));
        } else if (set.set_method() != null) {
            conf.set("bootstrap.method",set.set_method().bootstrap_method().getText().toLowerCase());
            info("Using method: " + conf.get("bootstrap.method"));
//        } else if (set.set_info() != null) {
//            transformer.setInfo(set.set_info().getChild(2).getText().toLowerCase().equals("on"));
//            q("Extra Info " + (isInfo() ? "ON" : "OFF"));
        } else if (set.set_sample_type() != null) {
            conf.set("bootstrap.sample-type",set.set_sample_type().getChild(3).getText().toLowerCase());
            info("Using " + conf.get("bootstrap.sample-type").toUpperCase() + " Samples.");
        }
        return null;
    }
}
