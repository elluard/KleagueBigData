import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD

/**
  * Created by leehwangchun on 2017. 5. 25..
  */

case class MatchResult(year : Int, leagueTitle : String, matchID : Int, roundID : Int, matchDate : Date, matchTeam : String,
                       homeTeam : String, awayTeam : String, homeScore : Int, awayScore : Int, resultScore : String, stadiumName : String)

class TeamResultLoader(inputFile : RDD[String])  {
  val resultList = inputFile.map{ line =>
    val data = line.split(",")
    val dateFormat = new SimpleDateFormat("yyyy.MM.dd")
    MatchResult(data(0).toInt, data(1), data(2).toInt, data(3).toInt, dateFormat.parse(data(4)), data(5),
      data(6), data(7), data(8).toInt, data(9).toInt, data(10), data(11))
  }

  def filter = resultList.filter(_)
}
