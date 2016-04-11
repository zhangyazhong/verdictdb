package edu.umich.tajik.verdict.hive.uda;

import edu.umich.tajik.verdict.hive.Poisson;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.HashMap;

public final class Sum extends UDAF {
    public static class Evaluator implements UDAFEvaluator {
        public static class SumAgg {
            double sum;

            public void add(double num) {
                byte p = Poisson.get();
                sum += num * p;
            }
        }

        private SumAgg buffer;

        public Evaluator() {
            init();
        }

        public void init() {
            buffer = new SumAgg();
        }

        public boolean iterate(Integer seed, Double val) {
            if (val != null)
                buffer.add(val);
            return true;
        }

        public SumAgg terminatePartial() {
            return buffer;
        }

        public boolean merge(SumAgg another) {
            //null might be passed in case there is no input data.
            if (another == null) {
                return true;
            }

            buffer.sum += another.sum;

            return true;
        }

        public double terminate() {
            return buffer.sum;
        }
    }
}