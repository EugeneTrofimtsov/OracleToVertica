package oracletovertica.target

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

class VerticaDialect  extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:vertica")

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    typeName match {
      case "Long Varchar" => Some(StringType)
      case "Integer" => Some(IntegerType)
      case "Timestamp" => Some(TimestampType)
      case "Boolean" => Some(BooleanType)
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case TimestampType => Some(JdbcType("DATETIME", java.sql.Types.TIMESTAMP))
    case StringType => Some(JdbcType("LONG VARCHAR", java.sql.Types.LONGVARCHAR))
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case _ => None
  }

}
