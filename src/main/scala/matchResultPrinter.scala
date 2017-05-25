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


    val pohangResult = new TeamSeasonResult("포항", result.filter(_.year == 2015))
    var roundResult : List[SeasonBaseData] = List()
    for(i <- 1 to 10)
      roundResult ++= List(pohangResult.seasonBaseDataToRound(i))

    roundResult.foreach(println)

    /*
    val awayResult = result.filter{ line => line.year == 2010 && line.awayTeam.contains("포항") }.collect()
    val awayScore = awayResult.foldLeft(TotalMatchResult(0,0,0,0,0)) { (totalResult, matchResult) =>
      val win = if (matchResult.awayScore > matchResult.homeScore) 1 else 0
      val lose = if (matchResult.awayScore < matchResult.homeScore) 1 else 0
      val draw = if (matchResult.awayScore == matchResult.homeScore) 1 else 0
      TotalMatchResult(totalResult.win + win, totalResult.draw + draw, totalResult.lose + lose, totalResult.getScore + matchResult.awayScore, totalResult.lossScore + matchResult.homeScore)
    }

    val homeResult = result.filter{ line => line.year == 2010 && line.homeTeam.contains("포항") }.collect()
    val homeScore = homeResult.foldLeft(TotalMatchResult(0,0,0,0,0)) { (totalResult, matchResult) =>
      val win = if (matchResult.homeScore > matchResult.awayScore) 1 else 0
      val lose = if (matchResult.homeScore < matchResult.awayScore) 1 else 0
      val draw = if (matchResult.homeScore == matchResult.awayScore) 1 else 0
      TotalMatchResult(totalResult.win + win, totalResult.draw + draw, totalResult.lose + lose, totalResult.getScore + matchResult.homeScore, totalResult.lossScore + matchResult.awayScore)
    }

    val awayResult2 = result.filter{ line => line.year == 2010 && line.awayTeam.contains("포항") }
    val homeResult2 = result.filter{ line => line.year == 2010 && line.homeTeam.contains("포항") }
    val totalResult2 = awayResult2.union(homeResult2)

    println("homeScore : " + homeScore)
    println("awayScore : " + awayScore)
    totalResult2.foreach(println)
    */
  }
}
