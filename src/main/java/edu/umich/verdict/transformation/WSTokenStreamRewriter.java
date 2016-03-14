package edu.umich.verdict.transformation;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.misc.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

//TODO: replace with a better one or remove
public class WSTokenStreamRewriter extends TokenStreamRewriter {
    private static List<String> keywords = Arrays.asList("select as from join where group by having limit".split(" "));
    private static List<String> newLiners = Arrays.asList("from where group having limit".split(" "));

    public WSTokenStreamRewriter(CommonTokenStream tokens) {
        super(tokens);
    }

    public String getText(String programName, Interval interval) {
        List rewrites = (List)this.programs.get(programName);
        int start = interval.a;
        int stop = interval.b;
        if(stop > this.tokens.size() - 1) {
            stop = this.tokens.size() - 1;
        }

        if(start < 0) {
            start = 0;
        }

        if(rewrites != null && !rewrites.isEmpty()) {
            StringBuilder buf = new StringBuilder();
            Map indexToOp = this.reduceToSingleOperationPerIndex(rewrites);
            int i = start;

            while(i <= stop && i < this.tokens.size()) {
                TokenStreamRewriter.RewriteOperation op = (TokenStreamRewriter.RewriteOperation)indexToOp.get(Integer.valueOf(i));
                indexToOp.remove(Integer.valueOf(i));
                Token op1 = this.tokens.get(i);
                if(op == null) {
                    if(op1.getType() != -1) {
                        String str = op1.getText();
                        if(newLiners.contains(str))
                            buf.append("\n");
                        buf.append(keywords.contains(str.toLowerCase())?str.toUpperCase():str);
                        buf.append(" ");
                    }

                    ++i;
                } else {
                    i = op.execute(buf);
                    buf.append(" ");
                }
            }

//            if(stop == this.tokens.size() - 1) {
//                Iterator var12 = indexToOp.values().iterator();
//
//                while(var12.hasNext()) {
//                    TokenStreamRewriter.RewriteOperation var11 = (TokenStreamRewriter.RewriteOperation)var12.next();
//                    if(var11.index >= this.tokens.size() - 1) {
//                        buf.append(var11.text);
//                    }
//                }
//            }

            return buf.toString();
        } else {
            return this.tokens.getText(interval);
        }
    }

}
