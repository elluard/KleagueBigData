import org.apache.spark.rdd.RDD

/**
  * Created by leehwangchun on 2017. 5. 25..
  */

case class SeasonBaseData(win : Int, draw : Int, lose : Int)

class TeamSeasonResult(teamName : String, matchResult : RDD[MatchResult]) extends Serializable{
  val winResult : RDD[MatchResult] = matchResult.filter { result =>
      (result.homeTeam.contains(teamName) && result.homeScore > result.awayScore) ||
      (result.awayTeam.contains(teamName) && result.awayScore > result.homeScore)
  }
  val loseResult : RDD[MatchResult] = matchResult.filter { result =>
    (result.homeTeam.contains(teamName) && result.homeScore < result.awayScore) ||
      (result.awayTeam.contains(teamName) && result.awayScore < result.homeScore)
  }
  val drawResult : RDD[MatchResult] = matchResult.filter { result =>
    (result.homeTeam.contains(teamName) || result.awayTeam.contains(teamName)) && result.homeScore == result.awayScore
  }

  def seasonBaseData : SeasonBaseData = {
    //여기서는 승/무/패/득점/실점 으로 구분한다.
    val winCount = winResult.count().toInt
    val loseCount = loseResult.count().toInt
    val drawCount = drawResult.count().toInt
    SeasonBaseData(winCount, loseCount, drawCount)
  }
}
