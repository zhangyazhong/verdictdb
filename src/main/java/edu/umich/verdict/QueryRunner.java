//package edu.umich.tajik.verdict;
//
//import edu.umich.tajik.verdict.models.ParsedStatement;
//import edu.umich.tajik.verdict.models.ResultSetWrapper;
//import edu.umich.tajik.verdict.parser.HplsqlParser;
//import edu.umich.tajik.verdict.view.ResultWriter;
//
//import java.sql.ResultSet;
//import java.sql.SQLException;
//
//public class QueryRunner {
//    private final DbConnector con;
//    private boolean info = true;
//    private final MetaDataManager metaDataManager;
//    private QueryTransformer transformer;
//
//    public QueryRunner(DbConnector connector) {
//        this.con = connector;
//        metaDataManager = connector.getMetaDataManager();
//        transformer = new QueryTransformer(metaDataManager);
//    }
//
//    public ResultSet run(String q) throws Exception {
//        return run(new ParsedStatement(q));
//    }
//
//    public ResultSet run(ParsedStatement q) throws Exception {
//        ResultSet rs = null;
//        switch (q.getType()) {
//            case Select:
//                rs = runSelect(q);
//                break;
//            case CreateSample:
//                runCreateSample(q);
//                break;
//            case DeleteSample:
//                runDeleteSample(q);
//                break;
//            case ShowSamples:
//                runShowSamples();
//                break;
//            case Set:
//                runSetConfig(q);
//                break;
//            case Other:
//                rs = runOther(q);
//                break;
//        }
//        return rs;
//    }
//
//    protected void info(String msg) {
//        if (!info)
//            return;
//        System.out.println(msg);
//    }
//
//    private void runDeleteSample(ParsedStatement q) throws SQLException {
//        metaDataManager.deleteSample(((HplsqlParser.Delete_sample_stmtContext) q.tree.getChild(0).getChild(0))
//                .table_name().getText());
//    }
//
//    private ResultSet runOther(ParsedStatement q) throws SQLException {
//        ResultSet rs;
//        try {
//            rs = con.executeQuery(q.toString());
//        } catch (SQLException e) {
//            System.err.println("Error in executing query:");
//            throw e;
//        }
//        return rs;
//    }
//
//    private void runSetConfig(ParsedStatement q) {
//        HplsqlParser.Set_configContext set = (HplsqlParser.Set_configContext) q.tree.getChild(0).getChild(0);
//        if (set.set_bootstrap_trials() != null) {
//            setBootstrapRepeats(Integer.parseInt(set.set_bootstrap_trials().int_number().getText()));
//            info("BOOTSTRAP TRIALS = " + getBootstrapRepeats());
//        } else if (set.set_confidence() != null) {
//            setConfidence(Float.parseFloat(set.set_confidence().dec_number().getText()));
//            info("CONFIDENCE = " + getConfidence());
//        } else if (set.set_use_samples() != null) {
//            setUseSamples(set.set_use_samples().getChild(3).getText().toLowerCase().equals("on"));
//            info(isUseSamples() ? "Using samples when available." : "Using original tables.");
//        } else if (set.set_preferred_sample() != null) {
//            setPreferredSample(Float.parseFloat(set.set_preferred_sample().children.size() == 6 ? set.set_preferred_sample().getChild(4).getText() : set.set_preferred_sample().getChild(3).getText()) / 100);
//            info("PREFERRED SAMPLE SIZE: " + getPreferredSample() * 100 + "%");
//        } else if (set.set_method() != null) {
//            setMethod(set.set_method().bootstrap_method().getText().toLowerCase());
//            info("Using method: " + getMethod());
//        } else if (set.set_info() != null) {
//            setInfo(set.set_info().getChild(2).getText().toLowerCase().equals("on"));
//            info("Extra Info " + (isInfo() ? "ON" : "OFF"));
//        } else if (set.set_sample_type() != null) {
//            setSampleType(set.set_sample_type().getChild(3).getText().toLowerCase());
//            info("Using " + getSampleType().toUpperCase() + " Samples.");
//        }
//    }
//
//    private void runShowSamples() throws SQLException {
//        ResultWriter.writeResultSet(System.out, metaDataManager.getSamplesInfo());
//    }
//
//    private void runCreateSample(ParsedStatement q) throws Exception {
//        HplsqlParser.Create_sample_stmtContext tree = (HplsqlParser.Create_sample_stmtContext) q.tree.getChild(0).getChild(0);
//        try {
//            info("Creating sample...");
//            if (tree.T_STRATIFIED() != null) {
//                String[] cols = new String[tree.expr().size()];
//                for (int i = 0; i < cols.length; i++)
//                    cols[i] = tree.expr(i).getText();
//                metaDataManager.createSample(new Sample(tree.getChild(2).getText(), tree.getChild(4).getText(), Float
//                        .parseFloat(tree.getChild(6).getText()) / 100, 0, tree.T_POISSON() != null ? Integer.parseInt(tree.children.get(9).getText()) : 0, cols));
//            } else
//                metaDataManager.createSample(new Sample(tree.getChild(2).getText(), tree.getChild(4).getText(), Float
//                        .parseFloat(tree.getChild(6).getText()) / 100, 0, tree.T_POISSON() != null ? Integer.parseInt(tree.children.get(9).getText()) : 0));
//
//            info("Sample created.");
//        } catch (Exception e) {
//            System.err.println("Error in creating sample: ");
//            throw e;
//        }
//    }
//
//    private ResultSet runSelect(ParsedStatement q) throws SQLException {
//        QueryTransformer analyser;
//        ResultSet rs;
//        analyser = new QueryTransformer(q);
//        analyser.transform(this);
//        try {
//            if (q.changed) {
//                info("New ParsedStatement:");
//                info(q.toString());
//                info("\n");
//                info("Using Sample: " + analyser.info.sample.name + " Size: " + (analyser.info.sample.compRatio * 100) + "%" + " Type: " + (analyser.info.sample.stratified ? "Stratified" : "Uniform"));
//                info("Bootstrap Trials: " + getBootstrapRepeats());
//                info("Method: " + getMethod());
//                rs = con.executeQuery(q.toString());
//                if (!isUseConfIntUdf())
//                    rs = new ResultSetWrapper(rs, analyser.info);
//            } else {
//                info("Running the original query...");
//                rs = con.executeQuery(q.toString());
//            }
//        } catch (SQLException e) {
//            System.err.println("Error in executing query:");
//            throw e;
//        }
//        return rs;
//    }
//
//
//    public boolean isInfo() {
//        return info;
//    }
//
//    public void setInfo(boolean info) {
//        this.info = info;
//    }
//}
//
