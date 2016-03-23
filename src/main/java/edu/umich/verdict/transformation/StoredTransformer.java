package edu.umich.verdict.transformation;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.models.Sample;
import edu.umich.verdict.processing.SelectStatement;

public class StoredTransformer extends QueryTransformer {
    public StoredTransformer(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        super(conf, metaDataManager, q);
    }

    @Override
    protected boolean addBootstrapTrials() {
        for (int j = selectItems.size() - 1; j >= 0; j--) {
            SelectListItem item = selectItems.get(j);
            if (item.isSupportedAggregate()) {
                StringBuilder buf = new StringBuilder(", ");
                buf.append("verdict.conf_int(").append(confidence).append(", ").append(item.getScale()).append(", ");
                int trials = bootstrapTrials;
                if (transformed.getSample().poissonColumns < bootstrapTrials) {
                    trials = transformed.getSample().poissonColumns;
                    System.err.println("WARNING: Selected sample has just " + trials + " Poisson number columns, however bootstrap.trials is set to " + bootstrapTrials + " which is more than available Poisson number columns. Performing " + trials + " bootstrap trials...");
                }
                for (int i = 0; i < trials; i++)
                    buf.append(getTrialExpression(item, i + 1)).append(", ");
                buf.replace(buf.length() - 2, buf.length(), ")");
                buf.append(" AS CI_").append(item.getIndex()).append(" ");
                rewriter.insertAfter(selectList.stop, buf.toString());
            }
        }
        return true;
    }

    private String getTrialExpression(SelectListItem item, int i) {
        String pref = metaDataManager.getPossionColumnPrefix();
        switch (item.getAggregateType()) {
            case AVG:
                return "sum((" + item.getExpression() + ") * " + pref + i + ")/sum(" + pref + i + ")";
            case SUM:
                return "sum((" + item.getExpression() + ") * " + pref + i + ")";
            case COUNT:
                return "sum(" + pref + i + ")";
            default:
                return null;
        }
    }

    @Override
    protected Sample getPreferred(Sample first, Sample second) {
        if (second.poissonColumns < bootstrapTrials)
            return second.poissonColumns < first.poissonColumns ? first : second;
        else
            return second.poissonColumns > first.poissonColumns && first.poissonColumns >= bootstrapTrials ? first : second;
    }
}
