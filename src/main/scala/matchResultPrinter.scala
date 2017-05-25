import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leehwangchun on 2017. 5. 24..
  */
object matchResultEntryPoint {
  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("matchResult").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile("./resources/matchResult")

    val result = new TeamResultLoader(sc.textFile("./resources/matchResult"))
    result.filter{ line => line.year == 2009 && line.homeTeam.contains("포항") }.foreach(println)
  }
}
