package edu.umich.verdict.transformation;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.processing.SelectStatement;

import java.util.Random;

public class UdaTransformer extends QueryTransformer {
    Random rnd = new Random();

    public UdaTransformer(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        super(conf, metaDataManager, q);
    }

    @Override
    protected String getUniformTrialExpression(SelectListItem item, int trial) {
        return getUda(item) + "(" + getRandomSeed() + ", " + item.getInnerExpression() + ")";
    }

    @Override
    protected String getStratifiedTrialExpression(SelectListItem item, int trial) {
        if (item.getAggregateType() == TransformedQuery.AggregateType.SUM)
            return getUda(item) + "(" + getRandomSeed() + ", (" + item.getInnerExpression() + ") * " + sampleAlias + "." + metaDataManager.getWeightColumn() + ")";
        return getUda(item) + "(" + getRandomSeed() + ", " + item.getInnerExpression() + ", " + sampleAlias + "." + metaDataManager.getWeightColumn() + ")";
    }

    private int getRandomSeed() {
        return rnd.nextInt();
    }

    private String getUda(SelectListItem item) {
        if (stratifiedSample() && item.getAggregateType() != TransformedQuery.AggregateType.SUM && !metaDataManager.supportsUdfOverloading())
            // here the name of the UDA that supports weights differs
            return "verdict.poisson_w" + item.getAggregateType().toString().toLowerCase();
        return "verdict.poisson_" + item.getAggregateType().toString().toLowerCase();
    }
}