package edu.umich.verdict.transformation;

import edu.umich.verdict.models.Sample;
import edu.umich.verdict.processing.ParsedStatement;
import edu.umich.verdict.processing.SelectStatement;
import org.antlr.v4.runtime.TokenStreamRewriter;

import java.util.ArrayList;
import java.util.List;

public class TransformedQuery {
    private final ParsedStatement original;
    private final TokenStreamRewriter rewriter;
    private Sample sample = null;
    private ArrayList<AggregateInfo> aggregates = new ArrayList<>();
    private int originalCols;
    private int bootstrapRepeats;
    private double confidence;
    private String method;

    public TransformedQuery(SelectStatement original, int bootstrapRepeats, double confidence, String method){
        this.original = original;
        this.rewriter = original.getRewriter();
        this.bootstrapRepeats = bootstrapRepeats;
        this.confidence = confidence;
        this.method = method;
    }

    public String toString() {
        if (isChanged())
            return rewriter.getText().replaceAll("(;\\s*)+$", "");
        return original.toString();
    }

    public boolean isChanged() {
        return this.sample!=null && !this.aggregates.isEmpty();
    }

    public Sample getSample() {
        return sample;
    }

    public void setSample(Sample sample) {
        this.sample = sample;
    }

    public AggregateInfo addAggregate(AggregateType type, String expr, int column) {
        AggregateInfo aggr = new AggregateInfo(type, expr, column);
        getAggregates().add(aggr);
        return aggr;
    }

    public int getOriginalCols() {
        return originalCols;
    }

    public void setOriginalCols(int originalCols) {
        this.originalCols = originalCols;
    }

    public List<AggregateInfo> getAggregates() {
        return aggregates;
    }

    public int getBootstrapRepeats() {
        return bootstrapRepeats;
    }

    public double getConfidence() {
        return confidence;
    }

    public String getMethod() {
        return method;
    }

    public class AggregateInfo {
        private AggregateType type;
        private String expr;
        private int column;

        public AggregateInfo(AggregateType type, String expr, int column) {
            this.type = type;
            this.expr = expr;
            this.column = column;
        }

        public AggregateType getType() {
            return type;
        }

        public String getExpr() {
            return expr;
        }

        public int getColumn() {
            return column;
        }
    }

    public enum AggregateType {
        AVG,
        SUM,
        COUNT,
        OTHER,
        NONE
    }

//    private String getNodeText(ParseTree tree) {
//        if (tree.getChildCount() == 0) {
//            return tree instanceof TerminalNodeImpl && keywords.contains(tree.getText().toLowerCase()) ? tree.getText().toUpperCase() : tree.getText();
//        } else {
//            StringBuilder builder = new StringBuilder();
//
//            for (int i = 0; i < tree.getChildCount(); ++i) {
//                builder.append(getNodeText(tree.getChild(i)));
//                builder.append(tree instanceof HplsqlParser.Subselect_stmtContext ? "\n" : " ");
//            }
//
//            return builder.toString().trim();
//        }
//    }
}
