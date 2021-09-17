package oracletovertica.target

import oracletovertica.Parameters
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object IntegrityControl {

  /** Проверка уникальности записей */
  def checkRecordsUnique(df: DataFrame): Boolean = {
    df.drop("dt_load").dropDuplicates().count() == df.count()
  }

  /** Проверка записанных и считанных колонок */
  def checkColumnsNames(df: DataFrame, columns: Array[String]): Boolean = {
    columns.forall(col => df.columns.map(_.toUpperCase).contains(col))
  }

  /** Проверка количества конкретных значений в указанной колонке */
  def countColumnValue(df: DataFrame, column: String, value: Any): Long = {
    df.select(column).where(col(column) === value).count()
  }

  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = Logger.getLogger(this.getClass.getName)
    implicit val conf: Parameters = new Parameters(args)
    implicit val spark: SparkSession = SparkSession.builder.getOrCreate()
    for (fullname <- Seq(conf.VERTICA_CLIENT_TABLE, conf.VERTICA_CLIENT_PHONE_TABLE, conf.VERTICA_LOG_TABLE)) {
      val info = spark.read.format("jdbc").options(conf.verticaConnectionConfig(fullname)).load()
      if (checkRecordsUnique(info)) logger.info(s"Integrity control passed for table $fullname: no duplicates")
      else logger.warn(s"Integrity control fails for table $fullname: duplicates found")
      if (info.columns.contains("verify")) {
        val num = countColumnValue(info, "verify", false)
        if (num == 0) logger.info(s"Integrity control passed for table $fullname: 0 wrong values")
        else logger.warn(s"Integrity control fails for table $fullname: $num wrong values")
      }
    }
  }

}
