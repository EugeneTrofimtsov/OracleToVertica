package oracletovertica.source

import java.io._
import java.text.DecimalFormat
import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter

import org.apache.log4j.Logger
import net.liftweb.json._
import oracletovertica.Parameters
import oracletovertica.source.Data._
import oracletovertica.source.Entity._
import org.apache.spark.sql.{SaveMode, SparkSession}

object OracleWriter {

  private val rand = new scala.util.Random(System.nanoTime)

  def getRandomDate(from: LocalDate, to: LocalDate): String = {
    LocalDate.ofEpochDay(from.toEpochDay + rand.nextInt((to.toEpochDay - from.toEpochDay).toInt))
      .format(DateTimeFormatter.ofPattern("dd.MM.yyyy"))
  }

  def getRandomPhone: String = {
    val num1 = (rand.nextInt(7) + 1) * 100 + (rand.nextInt(8) * 10) + rand.nextInt(8)
    val num2 = rand.nextInt(743)
    val num3 = rand.nextInt(10000)
    val format3 = new DecimalFormat("000")
    val format4 = new DecimalFormat("0000")
    format3.format(num1) + "-" + format3.format(num2) + "-" + format4.format(num3)
  }

  def getOptional[T](result: T): Option[T] = if (rand.nextInt(2) == 0) Some(result) else None

  def getRandomValue(dt: Long)(implicit conf: Parameters, formats: DefaultFormats.type): Information = {
    val city = rand.nextInt(cities.length - 1)
    val msg = Message(
      firstnames(rand.nextInt(firstnames.length - 1)),
      lastnames(rand.nextInt(lastnames.length - 1)),
      patronymics.lift(rand.nextInt(patronymics.length - 1)),
      getOptional(getRandomDate(
        LocalDate.of(1950, 1, 1), LocalDate.of(2000, 1, 1)
      )),
      getOptional((for (_ <- 1 to 1 + rand.nextInt(3)) yield getRandomPhone).toArray),
      Address(
        getOptional((1000000 + rand.nextInt((9000000 - 1000000) + 1)).toString),
        cities(city),
        codes(city),
        streets(rand.nextInt(streets.length - 1)),
        rand.nextInt(100).toString,
        getOptional(rand.nextInt(1000).toString),
        statuses(rand.nextInt(statuses.length - 1))
      )
    )
    Information(
      dt,
      compactRender(Extraction.decompose(msg)),
      if (rand.nextInt(100) + 1 > conf.WRONG_VALUES_PERCENT) msg.getHash(dt) else msg.getWrongHash(dt)
    )
  }

  def loadToFile(info: String)(implicit formats: DefaultFormats.type): Unit = {
    val fw = new FileWriter("test.json")
    fw.write(info)
    fw.close()
  }

  def loadToOracle[T <: scala.Product](info: Seq[T], fullname: String, columns: Seq[String])
                                      (implicit spark: SparkSession, conf: Parameters, logger: Logger,
                                       evidence$3: scala.reflect.runtime.universe.TypeTag[T]): Unit = {
    logger.info(s"Started writing to oracle table '$fullname'...")
    try {
      spark.createDataFrame(info).toDF(columns: _*).write
        .format("jdbc")
        .options(conf.oracleConnectionConfig(fullname))
        .mode(SaveMode.Append)
        .save()
    } catch {
      case e: Exception =>
        logger.error(s"Problems while writing to oracle table '$fullname'")
        logger.error(e.getMessage)
    }
    logger.info(s"Writing to oracle table '$fullname' complete...")
    logger.info(s"Number of wrote records: ${info.length}")
  }

  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = Logger.getLogger(this.getClass.getName)
    implicit val conf: Parameters = new Parameters(args)
    implicit val formats: DefaultFormats.type = DefaultFormats
    implicit val spark: SparkSession = SparkSession.builder.getOrCreate()
    val dt = Instant.now.getEpochSecond
    for (num <- (for (_ <- 1 to conf.RECORDS_NUMBER / 10000) yield 10000) :+ (conf.RECORDS_NUMBER % 10000)) {
      val batch = for (_ <- 1 to num) yield getRandomValue(dt)
      loadToOracle[Information](batch, conf.ORACLE_SRC_TABLE, Seq("dt", "msg", "hashmsg"))
      loadToFile(prettyRender(Extraction.decompose(batch)))
    }
    loadToOracle[Control](Seq(Control(dt, conf.RECORDS_NUMBER)), conf.ORACLE_LOG_TABLE, Seq("dt", "cnt"))
  }

}
