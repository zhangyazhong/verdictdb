package edu.umich.tajik.verdict.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.*;

import java.util.Arrays;

public class ConfidenceInterval extends UDF {
    public Text evaluate(Double conf, final Double...  args) {
        int c = args.length;
        double[] nums = new double[c];
        for(int i=0;i<c;i++)
            nums[i]=args[i];
        Arrays.sort(nums);
        int margin = (int) (c * (1 - conf) / 2);
        double l = nums[margin], r = nums[c - margin - 1];
        return new Text("[" + l + "," + r + "]");
    }
}