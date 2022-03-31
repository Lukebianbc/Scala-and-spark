import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {
    def getOutLinks(input: String): String ={
        input.split(":")(0)
    }
    def getInLinks(input: String): String={
        input.split(":")(1)
    }
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        

        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions)
            // TODO
        var outRDD = links.map(getOutLinks)
        var inRDD = links.map(getInLinks).flatMap(line => line.split("\\s+")).filter(link =>link !="")
        var outCounts = outRDD.map(word => (word.toInt, 1)).reduceByKey(_+_)
        var inCounts = inRDD.map(word => (word.toInt, 1)).reduceByKey(_+_)
        
        println("check out")
        outCounts.foreach(println)
        println("check in")
        inCounts.foreach(println)
        
        val titles = sc
            .textFile(titles_file, num_partitions)
            // TODO
        val titlesRDD = titles.zipWithIndex().map{
            case(line, i) =>((i+1).toInt, line)
        }

        /* No Outlinks */
        val no_outlinks = ()
        println("[ NO OUTLINKS ]")
        // TODO
        val no_outRDD = titlesRDD.subtractByKey(outCounts)
        no_outRDD.sortByKey().take(10).foreach(println)
        
        /* No Inlinks */
        val no_inlinks = ()
        println("\n[ NO INLINKS ]")
        // TODO
        val no_inRDD = titlesRDD.subtractByKey(inCounts)
        no_inRDD.sortByKey().take(10).foreach(println)
    }
}
