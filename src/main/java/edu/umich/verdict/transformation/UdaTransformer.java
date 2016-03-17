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
    protected boolean addBootstrapTrials() {
        for (int j = selectItems.size() - 1; j >= 0; j--) {
            SelectListItem item = selectItems.get(j);
            if (item.isSupportedAggr) {
                String uda = getUda(item);
                String expr = item.expr;
                StringBuilder buf = new StringBuilder(", ");
                //TODO: also handle without conf_int
                buf.append("verdict.conf_int(").append(confidence).append(", ").append(item.getScale()).append(", ");
                for (int i = 0; i < bootstrapTrials; i++)
                    buf.append(uda).append("(").append(getRandomSeed()).append(", ").append(expr).append("), ");
                buf.replace(buf.length() - 2, buf.length(), ")");
                buf.append(" AS CI_").append(item.index).append(" ");
                rewriter.insertAfter(selectList.stop, buf.toString());
            }
        }
        return true;
    }

    private int getRandomSeed() {
        return rnd.nextInt();
    }

    private String getUda(SelectListItem item) {
        //TODO: better names
        return "verdict.my_" + item.aggregateType.toString().toLowerCase();
    }

}
