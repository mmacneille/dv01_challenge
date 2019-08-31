import java.lang
import scala.collection.JavaConverters._

import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

object Dv01Challenge {

  //FIXME we would like this to be program input
  val filePath = "./LoanStats_securev1_2018Q4.csv"
  val outPath = "./LoanMetrics_2018Q4"
  val requiredColumns = Map(
    "grade" -> 8,
    "loan_amnt" -> 2,
    "loan_status" -> 16,
    "int_rate" -> 6,
    "total_rec_prncp" -> 42,
    "total_rec_int" -> 43,
    "chargeoff_within_12_mths" -> 83
  )

  def main(args: Array[String]): Unit = {
    computeLoanDetailMetricsForFile(filePath)
  }


  def computeLoanDetailMetricsForFile(filePath : String) = {
    val env = ExecutionEnvironment.getExecutionEnvironment

//    FIXME we would like to use this, but there are time consuming errors here
//    val loanDataSet = env.readCsvFile(filePath,includedFields = requiredColumns.map(_._2).toArray,pojoFields = requiredColumns.map(_._1).toArray)

    val loanFile = env.readTextFile(filePath)
    val loanRowDataSetWithErrors = loanFile.map(line => {
      try{
        Left(LoanRow(line))
      } catch {
        case e : LoanRow.LineReadException => Right(e)
      }

    })
    val loanRowDataSet = loanRowDataSetWithErrors.flatMap(s => s match {
      case Left(loanRow) => Some(loanRow)
      case Right(_) => None
    })

    val loanDataGroupedByGrade = loanRowDataSet.groupBy("grade")
    val loanMetricsByGroup = loanDataGroupedByGrade.reduceGroup(new LoanGroupReduceFn)

    //FIXME would be nice if maybe we had a prettier output format
    loanMetricsByGroup.map(_.toCsvString).writeAsText(outPath)
    env.execute()
  }

}

class LoanGroupReduceFn extends GroupReduceFunction[LoanRow,LoanMetric] with GroupCombineFunction[LoanMetric,LoanMetric]{
  override def reduce(values: lang.Iterable[LoanRow], out: Collector[LoanMetric]): Unit = {
    var metric : LoanMetric = null
    for(row <- values.asScala){
      if(metric == null){
        metric = LoanMetric(row.grade)
      }
      metric = metric.update(row)
    }
    out.collect(metric)
  }

  override def combine(values: lang.Iterable[LoanMetric], out: Collector[LoanMetric]): Unit = {
    val input = values.asScala
    //group metrics by their grades, add them together and collect the new ones
    input.groupBy(_.grade).map({
      case (_,metrics) => metrics.reduce((m1,m2) => m1.add(m2))
    }).foreach(metric => out.collect(metric))
  }
}



//TODO we will add adj net annualized return later
case class LoanMetric(grade: String,
                      totalIssued: Double,
                      fullyPaid : Double ,
                      current : Double,
                      late: Double,
                      chargedOff : Double,
                      principalReceived : Double,
                      interestReceived : Double,
                      avgInterestRate : Double,
                      loanCount : Int){

  def toCsvString = s"$grade,$totalIssued,$fullyPaid,$current,$late,$chargedOff,$principalReceived,$interestReceived,$avgInterestRate"

  def update(row : LoanRow):LoanMetric = {
    val newTotalIssued = totalIssued + row.loanAmt
    val newFullyPaid = if(row.loanStatus.contains("Fully Paid")) fullyPaid + row.loanAmt else fullyPaid
    val newCurrent = if(row.loanStatus.contains("Current")) current + row.loanAmt else current
    val newLate = if(row.loanStatus.contains("Late")) late + row.loanAmt else late
    val newChargedOff = if(row.chargeOffWithin12 > 0) chargedOff + row.loanAmt else chargedOff
    val newPrincipalReceived = principalReceived + row.totalReceivedPrncp
    val newInterestReceived = interestReceived + row.totalReceivedInt
    //FIXME this is not the right way to add onto the weighted average, do this later
    val newAvgInterestRate = avgInterestRate + (row.intRate / loanCount + 1)
    LoanMetric(grade,newTotalIssued,newFullyPaid,newCurrent,newLate,newChargedOff,newPrincipalReceived,newInterestReceived,newAvgInterestRate,loanCount + 1)
  }

  def add(other : LoanMetric): LoanMetric = {
    val newTotalIssued = totalIssued+other.totalIssued
    val newFullyPaid = fullyPaid + other.fullyPaid
    val newCurrent = current + other.current
    val newLate = late + other.late
    val newChargedOff = chargedOff + other.chargedOff
    val newPrincipalReceived = principalReceived + other.principalReceived
    val newInterestReceived = interestReceived + other.interestReceived
    //FIXME this is not the correct way to calculate weighted average interest rate, do this later
    val newAvgInterestRate = (avgInterestRate + other.avgInterestRate) / 2

    LoanMetric(grade,newTotalIssued,newFullyPaid,newCurrent,newLate,newChargedOff,newPrincipalReceived,newInterestReceived,newAvgInterestRate,loanCount + other.loanCount)
  }


}

object LoanMetric{
  def apply(grade: String):LoanMetric = LoanMetric(grade,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0)
}


case class LoanRow(grade: String,
                   loanAmt : Double,
                   loanStatus : String,
                   intRate : Double,
                   totalReceivedPrncp : Double,
                   totalReceivedInt : Double,
                   chargeOffWithin12 : Int)



object LoanRow {
  val requiredColumns = Map(
    "grade" -> 8,
    "loan_amnt" -> 2,
    "loan_status" -> 16,
    "int_rate" -> 6,
    "total_rec_prncp" -> 42,
    "total_rec_int" -> 43,
    "chargeoff_within_12_mths" -> 83
  )



  def apply(s: String):LoanRow = {
    val split = s.split(",").map(_.replaceAll("\"",""))
    try{
      val grade = split(requiredColumns("grade"))
      val loanAmt = split(requiredColumns("loan_amnt")).toDouble
      val loanStatus = split(requiredColumns("loan_status"))
      val intRateStr = split(requiredColumns("int_rate"))
      val intRate = intRateStr.replaceAll("%","").toDouble
      val totalRecPrincp = split(requiredColumns("total_rec_prncp")).toDouble
      val totalRecInt = split(requiredColumns("total_rec_int")).toDouble
      val chargeOffWithin12 = split(requiredColumns("chargeoff_within_12_mths")).toInt
      LoanRow(grade,loanAmt,loanStatus,intRate,totalRecPrincp,totalRecInt,chargeOffWithin12)
    } catch {
      case e : Exception => throw LineReadException(split.length,s)
    }

  }

  case class LineReadException(splitLength : Int,line : String) extends Exception


}