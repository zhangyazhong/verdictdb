package edu.umich.tajik.verdict.hive.uda;

import edu.umich.tajik.verdict.hive.Poisson;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.HashMap;

public final class Count extends UDAF {
    public static class Evaluator implements UDAFEvaluator {
        public static class CountAgg {
            double sum;

            public void add() {
                sum += Poisson.get();
            }
        }

        private CountAgg buffer;

        public Evaluator() {
            init();
        }

        public void init() {
            buffer = new CountAgg();
        }

        public boolean iterate(Integer extra) {
            buffer.add();
            return true;
        }

        public CountAgg terminatePartial() {
            return buffer;
        }

        public boolean merge(CountAgg another) {
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