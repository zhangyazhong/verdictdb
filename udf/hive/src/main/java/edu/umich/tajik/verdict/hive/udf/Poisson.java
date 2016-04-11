package edu.umich.tajik.verdict.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.lang.Integer;
import java.util.Random;


public class Poisson extends UDF {
    static double probs[] = {
            0
            , 0.3678794469446667
            , 0.7357588622887303
            , 0.9196985778609128
            , 0.981011816384974
            , 0.9963401260159892
            , 0.999405787942192
            , 0.9999167718585252
            , 0.9999897235471829
            , 0.9999988878062837
    };
    static Random rnd= new Random();

    public byte evaluate(Integer seed){
        return evaluate();
    }

    public byte evaluate() {
        double frac = rnd.nextDouble();
        if (frac <= probs[1])
            return (byte) 0;
        else if (frac <= probs[2])
            return (byte) 1;
        else if (frac <= probs[3])
            return (byte) 2;
        else if (frac <= probs[4])
            return (byte) 3;
        else if (frac <= probs[5])
            return (byte) 4;
        else if (frac <= probs[6])
            return (byte) 5;
        else if (frac <= probs[7])
            return (byte) 6;
        else if (frac <= probs[8])
            return (byte) 7;
        else if (frac <= probs[9])
            return (byte) 8;
        else return (byte) 9;
    }
}