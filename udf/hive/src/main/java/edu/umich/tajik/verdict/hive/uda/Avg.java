package edu.umich.tajik.verdict.hive.uda;

import edu.umich.tajik.verdict.hive.Poisson;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.HashMap;

public final class Avg extends UDAF {
    public static class Evaluator implements UDAFEvaluator {
        public static class AvgAgg {
            double sum;
            long count;

            public void add(double num) {
                byte p = Poisson.get();
                sum += num * p;
                count += p;
            }
        }

        private AvgAgg buffer;

        public Evaluator() {
            init();
        }

        public void init() {
            buffer = new AvgAgg();
        }

        public boolean iterate(Integer seed, Double val) {
            if (val != null)
                buffer.add(val);
            return true;
        }

        public AvgAgg terminatePartial() {
            return buffer;
        }

        public boolean merge(AvgAgg another) {
            //null might be passed in case there is no input data.
            if (another == null) {
                return true;
            }

            buffer.sum += another.sum;
            buffer.count += another.count;

            return true;
        }

        public double terminate() {
            return buffer.sum / buffer.count;
        }
    }
}