package oracletovertica

import org.apache.log4j.Logger

class Parameters(args: Array[String])(implicit logger: Logger) {

  val paramMap: Map[String, String] = args.flatMap(argLine => argLine.split("==", 2) match {
    case Array(key, value) if value.nonEmpty => Some(key -> value)
    case _ =>
      logger.warn(s"Cannot parse parameter string or empty value: $argLine")
      None
  }).toMap

  logger.info("=====" * 4)
  paramMap.foreach {
    case (k, v) => logger.info(s"$k = $v")
  }
  logger.info("=====" * 4)

  /** Oracle common parameters */
  val ORACLE_JDBC_URL: String = paramMap.getOrElse("JDBC_URL", "")
  val ORACLE_JDBC_DRIVER: String = paramMap.getOrElse("JDBC_DRIVER", "oracle.jdbc.driver.OracleDriver")
  val ORACLE_USER: String = paramMap.getOrElse("USER", "")
  val ORACLE_PASSWORD: String = paramMap.getOrElse("PASSWORD", "")
  val RECORDS_NUMBER: Int = paramMap.getOrElse("RECORDS_NUMBER", "10000").toInt
  val WRONG_VALUES_PERCENT: Int = paramMap.getOrElse("WRONG_VALUES_PERCENT", "0").toInt
  val ORACLE_SRC_TABLE: String = paramMap.getOrElse("ORACLE_SRC_TABLE", "pt_json")
  val ORACLE_LOG_TABLE: String = paramMap.getOrElse("ORACLE_LOG_TABLE", "pt_json_control")

  /** Vertica common parameters */
  val VERTICA_JDBC_URL: String = paramMap.getOrElse("JDBC_URL", "")
  val VERTICA_JDBC_DRIVER: String = paramMap.getOrElse("JDBC_DRIVER", "com.vertica.jdbc.Driver")
  val VERTICA_USER: String = paramMap.getOrElse("USER", "")
  val VERTICA_PASSWORD: String = paramMap.getOrElse("PASSWORD", "")
  val TABLES_TO_LOAD: String = paramMap.getOrElse("TABLES_TO_LOAD", "all")
  val LOAD_TYPE: String = paramMap.getOrElse("LOAD_TYPE", "incremental")
  val VERTICA_CLIENT_TABLE: String = paramMap.getOrElse("VERTICA_CLIENT_TABLE", "pt_client")
  val VERTICA_CLIENT_PHONE_TABLE: String = paramMap.getOrElse("VERTICA_CLIENT_PHONE_TABLE", "pt_client_phone")
  val VERTICA_LOG_TABLE: String = paramMap.getOrElse("VERTICA_LOG_TABLE", "pt_json_control")

  def oracleConnectionConfig(query: String): Map[String, String] = Map(
    "url" -> ORACLE_JDBC_URL,
    "driver" -> ORACLE_JDBC_DRIVER,
    "dbtable" -> query,
    "user" -> ORACLE_USER,
    "password" -> ORACLE_PASSWORD
  )

  def verticaConnectionConfig(query: String): Map[String, String] = Map(
    "url" -> VERTICA_JDBC_URL,
    "driver" -> VERTICA_JDBC_DRIVER,
    "dbtable" -> query,
    "user" -> VERTICA_USER,
    "password" -> VERTICA_PASSWORD
  )

}
