package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.VerdictException;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.jdbc.VStatement;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ParsedStatement {
    protected String str;
    protected ParseTree tree;

    public ParsedStatement(String str, ParseTree tree){
        this.str = str;
        this.tree = tree;
    }

    //TODO: use customized exceptions
    public ResultSet run(Configuration conf, DbConnector connector) throws SQLException, VerdictException {
        ResultSet rs;
        try {
            String sql = this.toString().trim();
            rs = connector.executeQuery(sql);
            //TODO: make a separate statement for "USE schema"
            if(sql.toLowerCase().startsWith("use "))
                connector.getMetaDataManager().setCurrentSchema(sql.split(" ")[1]);
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
