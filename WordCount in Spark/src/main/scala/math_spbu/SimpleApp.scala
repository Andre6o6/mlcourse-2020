package math_spbu
import org.apache.spark.SparkContext
import java.io._

object SimpleApp {
  def main(args: Array[String]): Unit = {
    var sc = new SparkContext("local[*]", "Example")
    var text = sc.textFile("src/main/data/ShadowOverInnsmouth.txt")
    val counts = text.flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(_._2, ascending = false)
      .collect()
    //counts.slice(0,10).foreach(println)

    val pw = new PrintWriter(new File("out.txt" ))
    for ((w, c) <- counts)
      pw.println(w + " -  " + c)
    pw.close
  }
}