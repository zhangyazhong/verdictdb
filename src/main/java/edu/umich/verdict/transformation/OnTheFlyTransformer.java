package edu.umich.verdict.transformation;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.models.Sample;
import edu.umich.verdict.processing.SelectStatement;

public class OnTheFlyTransformer extends QueryTransformer {
    public OnTheFlyTransformer(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        super(conf, metaDataManager, q);
    }

    @Override
    protected boolean addBootstrapTrials() {
        for (int j = selectItems.size() - 1; j >= 0; j--) {
            SelectListItem item = selectItems.get(j);
            if (item.isSupportedAggr) {
                StringBuilder buf = new StringBuilder(", ");
                //TODO: also handle without conf_int
                buf.append("verdict.conf_int(").append(confidence).append(", ").append(item.getScale()).append(", ");
                for (int i = 0; i < bootstrapTrials; i++)
                    buf.append(getTrialExpression(item, i + 1)).append(", ");
                buf.replace(buf.length() - 2, buf.length(), ")");
                buf.append(" AS CI_").append(item.index).append(" ");
                rewriter.insertAfter(selectList.stop, buf.toString());
            }
        }
        return true;
    }

    private String getTrialExpression(SelectListItem item, int i) {
        String pref = metaDataManager.getPossionColumnPrefix();
        switch (item.aggregateType) {
            case AVG:
                return "sum((" + item.expr + ") * verdict.poisson())/sum(verdict.poisson())";
            case SUM:
                return "sum((" + item.expr + ") * verdict.poisson())";
            case COUNT:
                return "sum(verdict.poisson())";
            default:
                return null;
        }
    }
}
