import org.apache.spark.rdd.RDD

/**
  * Created by leehwangchun on 2017. 5. 25..
  */

case class SeasonBaseData(win : Int, draw : Int, lose : Int, getScore : Int, lossScore : Int)

class TeamSeasonResult(teamName : String, matchResult : RDD[MatchResult]) extends Serializable{
  val homeMatch : RDD[MatchResult] = matchResult.filter(_.homeTeam.contains(teamName))
  val awayMatch : RDD[MatchResult] = matchResult.filter(_.awayTeam.contains(teamName))
  val startRoundID : Int = matchResult.map(_.roundID).min()
  val endRoundID : Int = matchResult.map(_.roundID).max()

  private def foldSeasonBaseData(a : SeasonBaseData, b : SeasonBaseData) : SeasonBaseData =
    SeasonBaseData(a.win + b.win, a.draw + b.draw, a.lose + b.lose, a.getScore + b.getScore, a.lossScore + b.lossScore)

  def roundRangeData(fromRound : Int)(toRound : Int) : SeasonBaseData = {
    //여기서는 승/무/패/득점/실점 으로 구분한다.
    //filter 문이 돌면서 전체 loop를 한번 더 도는지 확인해봐야함
    //lazy val 의 리스트(aka 스트림) 으로 구성이 되어있다면 큰 문제가 없겠지만..
    val homeResult = homeMatch.filter(a => fromRound <= a.roundID && a.roundID <= toRound).map{ matchResult =>
      val win = if(matchResult.homeScore > matchResult.awayScore) 1 else 0
      val draw = if(matchResult.homeScore == matchResult.awayScore) 1 else 0
      val lose = if(matchResult.homeScore < matchResult.awayScore) 1 else 0
      SeasonBaseData(win, draw, lose, matchResult.homeScore, matchResult.awayScore)
    }.collect()

    val awayResult = awayMatch.filter(a => fromRound <= a.roundID && a.roundID <= toRound).map{ matchResult =>
      val win = if(matchResult.awayScore > matchResult.homeScore) 1 else 0
      val draw = if(matchResult.awayScore == matchResult.homeScore) 1 else 0
      val lose = if(matchResult.awayScore < matchResult.homeScore) 1 else 0
      SeasonBaseData(win, draw, lose, matchResult.awayScore, matchResult.homeScore)
    }.collect()

    (homeResult ++ awayResult).fold(SeasonBaseData(0,0,0,0,0))(foldSeasonBaseData)
  }

  def seasonBaseData : SeasonBaseData = roundRangeData(startRoundID)(endRoundID)
  def seasonBaseDataToRound(to : Int) = roundRangeData(startRoundID)(to)
  def seasonBaseDataFromRount(from : Int) = roundRangeData(from)(endRoundID)
}
