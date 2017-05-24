import java.io.StringReader
import java.text.SimpleDateFormat
import java.util.Date

import com.opencsv.CSVReader
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leehwangchun on 2017. 5. 24..
  */
case class MatchResult(year : String, leagueTitle : String, matchID : Int, roundID : Int, matchDate : Date, matchTeam : String,
                      homeTeam : String, awayTeam : String, homeScore : Int, awayScore : Int, resultScore : String, stadiumName : String)

object matchResultEntryPoint {
  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("matchResult").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile("./resources/matchResult")

    val result = input.map { line =>
      val data = line.split(",")
      val dateFormat = new SimpleDateFormat("yyyy.MM.dd")
      MatchResult(data(0), data(1), data(2).toInt, data(3).toInt, dateFormat.parse(data(4)), data(5),
        data(6), data(7), data(8).toInt, data(9).toInt, data(10), data(11))
    }

    val result2 = result.map{csv => csv}

    result2.foreach(println)
  }
}
