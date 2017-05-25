import java.util.Date

import org.apache.spark.rdd.RDD

/**
  * Created by leehwangchun on 2017. 5. 25..
  */
//case class SeasonBaseData(win : Int, draw : Int, lose : Int, getScore : Int, lossScore : Int)
//case class MatchResult(year : Int, leagueTitle : String, matchID : Int, roundID : Int, matchDate : Date, matchTeam : String,
//homeTeam : String, awayTeam : String, homeScore : Int, awayScore : Int, resultScore : String, stadiumName : String)
case class WinLoseData(winOrLose : String, matchDate : Date, opposingTeam : String, getScore : Int, lossScore : Int, stadium : String)

class TeamWinLoseAnalysis(teamName : String, matchResultChunk : RDD[MatchResult]) extends Serializable {
  val winResult : RDD[WinLoseData] = extractWinResult(teamName, matchResultChunk)
  val loseResult : RDD[WinLoseData] = extractLoseResult(teamName, matchResultChunk)
  val drawResult : RDD[WinLoseData] = extractDrawResult(teamName, matchResultChunk)

  def makeWinLoseData(winOrLose : String, teamName: String, result : MatchResult) : WinLoseData = {
    WinLoseData(winOrLose, result.matchDate, result.opposingTeam(teamName), result.selfScore(teamName), result.opposingScore(teamName), result.stadiumName)
  }

  def extractWinResult(teamName : String, matchResult : RDD[MatchResult]) : RDD[WinLoseData] = {
    matchResult.filter(_.checkWin(teamName)).map(makeWinLoseData("Win", teamName, _))
  }

  def extractLoseResult(teamName : String, matchResult : RDD[MatchResult]) : RDD[WinLoseData] = {
    matchResult.filter(_.checkLose(teamName)).map(makeWinLoseData("Lose", teamName, _))
  }

  def extractDrawResult(teamName : String, matchResult: RDD[MatchResult]) : RDD[WinLoseData] = {
    matchResult.filter(_.checkDraw).map(makeWinLoseData("Draw", teamName, _))
  }

  private def showResultViaTeam(result : RDD[WinLoseData])(opposingTeam : String) : RDD[WinLoseData] = {
    result.filter(_.opposingTeam.contains(opposingTeam))
  }

  def showWinResultViaTeam : String => RDD[WinLoseData] = showResultViaTeam(winResult)(_)

  def showLoseResultViaTeam : String => RDD[WinLoseData] = showResultViaTeam(loseResult)(_)

  def showDrawResultViaTeam : String => RDD[WinLoseData] = showResultViaTeam(drawResult)(_)
}
