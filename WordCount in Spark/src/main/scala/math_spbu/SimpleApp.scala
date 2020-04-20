package math_spbu
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {
    var sc = new SparkContext("local[*]", "Example")
    var text = sc.textFile("src/main/data/ShadowOverInnsmouth.txt")

    val counts = text.flatMap(_.split(" "))
      .map(_.replaceAll("[^\\w]","").toLowerCase)
      .filter(!_.isEmpty)
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(_._2, ascending = false)

    counts.coalesce(1).saveAsTextFile("results_text")

    var sparkSession = SparkSession.builder().appName("Example").getOrCreate()
    val df = sparkSession.createDataFrame(counts).toDF("word", "count")
    df.coalesce(1)
      .write
      .option("header","true")
      .mode("overwrite")
      .csv("results_csv")
  }
}