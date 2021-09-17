package oracletovertica.target

import java.sql.Timestamp
import java.time.Instant

import net.liftweb.json._
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import oracletovertica.Parameters
import oracletovertica.source.Entity._
import oracletovertica.target.Entity._
import org.apache.spark.sql.jdbc.JdbcDialects

object VerticaWriter {

  def loadFromOracle(fullname: String)(implicit conf: Parameters, spark: SparkSession, logger: Logger): DataFrame = {
    logger.info(s"Started reading from oracle table '$fullname'...")
    var info = spark.emptyDataFrame
    try {
      info = spark.read.format("jdbc").options(conf.oracleConnectionConfig(fullname)).load()
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
    logger.info(s"Reading from oracle table '$fullname' complete...")
    info
  }

  def parseJson[T](jsonString: String)(implicit m: Manifest[T]): T = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val jsonValue = parse(jsonString)
    jsonValue.extract[T]
  }

  def formClientTable(data: DataFrame, schema: List[StructField])(implicit spark: SparkSession): DataFrame = {
    val encoder = RowEncoder(StructType(schema))
    data.map {
      elem =>
        val info = Information(elem.getDecimal(0).longValue(), elem.getString(1), elem.getString(2))
        val msg = parseJson[Message](info.msg)
        val hashctrl = msg.getHash(info.dt)
        Row(
          new Timestamp(info.dt * 1000L),
          new Timestamp(Instant.now.getEpochSecond * 1000L),
          msg.lastname + " " + msg.firstname + " " + msg.patronymic.getOrElse(""),
          msg.birthday.getOrElse(""),
          Seq(msg.address.city, msg.address.code, msg.address.zipcode.getOrElse(""),
            msg.address.street, msg.address.house, msg.address.apartment.getOrElse(""),
            msg.address.status).mkString(" "),
          msg.phone.getOrElse(Array.empty).length,
          info.hashmsg,
          hashctrl,
          info.hashmsg == hashctrl
        )
    } (encoder).selectExpr(schema.map(field => s"CAST ( ${field.name} As ${field.dataType.sql}) ${field.name}"): _*)
  }

  def formClientPhoneTable(data: DataFrame, schema: List[StructField])(implicit spark: SparkSession): DataFrame = {
    val encoder = RowEncoder(StructType(schema))
    data.flatMap {
      elem =>
        val info = Information(elem.getDecimal(0).longValue(), elem.getString(1), elem.getString(2))
        val msg = parseJson[Message](info.msg)
        for (phone <- msg.phone.getOrElse(Array.empty)) yield
          Row(
            new Timestamp(info.dt * 1000L),
            new Timestamp(Instant.now.getEpochSecond * 1000L),
            msg.lastname + " " + msg.firstname + " " + msg.patronymic.getOrElse(""),
            phone
          )
    } (encoder).selectExpr(schema.map(field => s"CAST ( ${field.name} As ${field.dataType.sql}) ${field.name}"): _*)
  }

  def formControlTable(data: DataFrame, schema: List[StructField])(implicit spark: SparkSession): DataFrame = {
    val encoder = RowEncoder(StructType(schema))
    data.map {
      elem =>
        val info = Control(elem.getDecimal(0).longValue(), elem.getDecimal(1).longValue())
        Row(
          new Timestamp(info.dt * 1000L),
          new Timestamp(Instant.now.getEpochSecond * 1000L),
          info.cnt
        )
    } (encoder).selectExpr(schema.map(field => s"CAST ( ${field.name} As ${field.dataType.sql}) ${field.name}"): _*)
  }

  def writeToVertica(data: DataFrame, fullname: String)
                    (implicit spark: SparkSession, conf: Parameters, logger: Logger): Unit = {
    logger.info(s"Started writing table $fullname to vertica...")
    conf.LOAD_TYPE match {
      case "incremental" =>
        data.write.format("jdbc")
          .options(conf.verticaConnectionConfig(fullname))
          .mode(SaveMode.Append)
          .save()
      case "overwrite" =>
        spark.sql(s"DROP TABLE IF EXISTS $fullname")
        data.write.format("jdbc")
          .options(conf.verticaConnectionConfig(fullname))
          .mode(SaveMode.Overwrite)
          .save()
      case "test" =>
        data.show(false)
    }
    logger.info(s"Vertica writing table $fullname complete...")
  }

  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = Logger.getLogger(this.getClass.getName)
    implicit val conf: Parameters = new Parameters(args)
    implicit val spark: SparkSession = SparkSession.builder.getOrCreate()
    JdbcDialects.registerDialect(new VerticaDialect)
    conf.TABLES_TO_LOAD match {
      case "all" =>
        val values = loadFromOracle(conf.ORACLE_SRC_TABLE)
        writeToVertica(formClientTable(values, clientSchema), conf.VERTICA_CLIENT_TABLE)
        writeToVertica(formClientPhoneTable(values, clientPhoneSchema), conf.VERTICA_CLIENT_PHONE_TABLE)
        val controlValues = loadFromOracle(conf.ORACLE_LOG_TABLE)
        writeToVertica(formControlTable(controlValues, controlSchema), conf.VERTICA_LOG_TABLE)
      case topicName if topicName == conf.ORACLE_SRC_TABLE =>
        val values = loadFromOracle(conf.ORACLE_SRC_TABLE)
        writeToVertica(formClientTable(values, clientSchema), conf.VERTICA_CLIENT_TABLE)
        writeToVertica(formClientPhoneTable(values, clientPhoneSchema), conf.VERTICA_CLIENT_PHONE_TABLE)
      case topicName if topicName == conf.ORACLE_LOG_TABLE =>
        val controlValues = loadFromOracle(conf.ORACLE_LOG_TABLE)
        writeToVertica(formControlTable(controlValues, controlSchema), conf.VERTICA_LOG_TABLE)
      case _ =>
        logger.error(s"No such table '${conf.TABLES_TO_LOAD}'")
    }
  }
}
