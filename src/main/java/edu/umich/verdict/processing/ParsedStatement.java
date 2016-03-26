package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.connectors.DbConnector;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ParsedStatement {
    protected String str;
    protected ParseTree tree;

    public ParsedStatement(String str, ParseTree tree){
        this.str = str;
        this.tree = tree;
    }

    //TODO: use customized exceptions
    public ResultSet run(Configuration conf, DbConnector connector) throws Exception {
        ResultSet rs;
        try {
            rs = connector.executeQuery(this.toString());
        } catch (SQLException e) {
            System.err.println("Error in executing query:");
            throw e;
        }
        return rs;
    }

    //TODO: use a logger
    protected void info(String msg) {
//        if (!q)
//            return;
        System.out.println(msg);
    }

    public ParseTree getParseTree(){
        return tree;
    }

    public String toString() {
        return str.replaceAll("(;\\s*)+$", "");
    }
}
