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

}
