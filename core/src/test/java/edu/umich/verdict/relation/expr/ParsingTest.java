package edu.umich.verdict.relation.expr;

import org.junit.Test;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.relation.SingleRelation;

public class ParsingTest {
	
	@Test
	public void test() throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("dummy");
		VerdictContext vc = VerdictContext.from(conf);
		
		Relation r = SingleRelation.from(vc, "orders")
					 .where(String.format("abs(fnv_hash(%s)) %% 10000 <= %.4f", "order_dow", 0.01*10000))
					 .select("*, round(rand(unix_timestamp())*100)%100 AS " + "verdict_partition");
		String sql = r.toSql();
		
		System.out.println(sql);
		
		System.out.println(Relation.prettyfySql(sql));
	}

}
