import org.apache.spark.sql.SparkSession

object HospitalCaseStudy{
  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.master("local")
      .appName("spark session example")
      .getOrCreate()

    val hospitalData = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("F:\\PDF Architect\\inpatientCharges.csv")
    hospitalData.show(5)
    import sparkSession.implicits._

    val totalDischarge = hospitalData.groupBy($"ProviderState",$"DRGDefinition")
        .sum("TotalDischarges").withColumnRenamed("sum(TotalDischarges)", "Sum")
    totalDischarge.orderBy(($"Sum").desc ).show()

   // val avgAmount = hospitalData.groupBy($"ProviderState").avg("AverageCoveredCharges").show()
//    val avgPayment = hospitalData.groupBy($"ProviderState").sum("AverageTotalPayments").show()
  //  val avgMedicarePayments = hospitalData.groupBy($"ProviderState").sum("AverageMedicarePayments").show()

  }
}