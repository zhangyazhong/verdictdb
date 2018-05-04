// for recording time cost for code running
def time[R](block: => R): R = {
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    println("\n\n--------------------------------")
    println("time cost:[" + ((end - start) / 1000000d) + "ms]")
    println("--------------------------------")
    result
}


// initialize environment
import edu.umich.verdict.VerdictSpark2Context
val vc = new VerdictSpark2Context(sc)


// show samples
vc.sql("show samples of wiki").show(false)


// create samples
vc.sql("create 1% sample of wiki.pagecounts_1e").show(false)


// delete samples
vc.sql("delete samples of wiki.pagecounts_1e").show(false)


// SQL statements
vc.sql("select avg(page_count) from wiki.pagecounts_1e").show(false)
