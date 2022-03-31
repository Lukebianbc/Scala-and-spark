import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links = sc
            .textFile(links_file, num_partitions)
            // TODO
        val outRDD = links.map{
          link =>
            val key = link.split(":")(0).toInt
            val value = link.split(":")(1).split("\\s+").tail.map(_.toInt)
            (key, value)
        }
        val titles = sc
            .textFile(titles_file, num_partitions)
            // TODO
        val titlesRDD = titles.zipWithIndex().map{
          case(input, i) =>
            ((i+1).toInt, input)
        }
        val N = titles.count
        var PR_0 = titlesRDD.mapValues(titles => 100.0/N) //use var instead of val
        var PR_i = PR_0
        var damping = 0.85
        /* PageRank */
        for (i <- 1 to iters) {
            val prevPR = PR_0.join(outRDD).values.flatMap{
              case(rank, links) => links.map(link=>(link, rank/links.size))
            }
            PR_i = prevPR.reduceByKey(_+_).mapValues( ((1-damping)*100.0/N) +damping*_)
            PR_0 = PR_i ++ PR_0.subtractByKey(PR_i).mapValues(x =>(1-damping)*100.0/N)
        }

        println("[ PageRanks ]")
        // TODO
        val sum = PR_0.map(_._2).sum()//long type
        val rank = PR_0.mapValues(x =>x*100/sum)
        val RDD = titlesRDD.join(rank).map{
          case(index, (link, value)) =>(index, (link, value))
        }
        RDD.sortBy(r=>(r._2._2,-r._1), false).take(11).foreach(println)
    }
}
