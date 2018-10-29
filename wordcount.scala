import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount{
def main(args: Array[String]) {
//set conf and sc
val conf = new SparkConf().setAppName("Word Count")
val sc = new SparkContext(conf)
//load file one.txt and two.txt
val file = sc.textFile("./one.txt,two.txt")//
val counts = file.flatMap(line=> line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
//change result to rdd
val result_rdd1 = sc.parallelize(counts.collect())
//save
result_rdd1.saveAsTextFile("./wcOutput")


}
}
