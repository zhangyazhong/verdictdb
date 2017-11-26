package edu.umich.verdict.relation;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ParsingTest {

    @Test
    public void test() throws VerdictException {
        
        VerdictConf conf = new VerdictConf();
        conf.setDbms("dummy");
        VerdictJDBCContext vc = VerdictJDBCContext.from(conf);
        
        String sql = "select * from mytable t lateral view explode(mycolumn_list) s as mycolumn";
        ExactRelation r = ExactRelation.from(vc, sql);
        System.out.println(r.toSql());
    }
    
    @Test
    public void test2() throws VerdictException {
    	final String host = "salat1.eecs.umich.edu";
        final String port = "21050";
        final String schema = "gaddepa2";

        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(host);
        conf.setPort(port);
        conf.setDbmsSchema(schema);
        conf.set("verdict.loglevel", "debug");
        conf.set("verdict.meta_data.meta_database_suffix", "_verdict");
        VerdictJDBCContext vc = VerdictJDBCContext.from(conf);
    	
    	String sql = "select count(*) from cbb_simple_yongjoo lateral view explode(multinomial_dept_list) t as multinomial_dept";
//        ExactRelation r = ExactRelation.from(vc, sql);
//        System.out.println(r.toSql());
        vc.executeJdbcQuery(sql);
    }

}
