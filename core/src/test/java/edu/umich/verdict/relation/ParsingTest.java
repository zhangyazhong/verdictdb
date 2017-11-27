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
        vc.executeJdbcQuery(sql);
    }
    
    @Test
    public void test3() throws VerdictException {
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
        
        String sql = "select count(*) as totalcount\n" + 
                "from \n" + 
                "  (select xTable.*,\n" + 
                "          Col1\n" + 
                "   from\n" + 
                "     (select pTable.*,\n" + 
                "             dept AS Col2\n" + 
                "      from\n" + 
                "        (select *\n" + 
                "         from cbb_simple_yongjoo\n" + 
                "         where process_date = '9/28/2017') pTable LATERAL VIEW explode(multinomial_dept_list) deptT AS dept\n" + 
                "     ) xTable\n" + 
                "     LATERAL VIEW explode(frozen_segment_names) pdateT AS Col1\n" + 
                "  ) segTable\n" + 
                "where Col1 = 'seg'\n";
        vc.executeJdbcQuery(sql);
    }

}
