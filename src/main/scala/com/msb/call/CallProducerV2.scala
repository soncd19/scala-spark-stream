package com.msb.call

import com.msb.utils.{Logging, StringUtils}
import oracle.jdbc.internal.OracleCallableStatement

import java.io.{Serializable, StringReader}
import java.sql.{Clob, Connection, SQLException, Types}

class CallProducerV2(connection: Connection) extends Serializable with Logging {


  @throws[SQLException]
  def close(): Unit = {
    connection.close()
  }

  @throws[Exception]
  def callProduce(pAction: String, pClob: String): String = {
    var l_err_level = 0L
    var l_clob002: Clob = null
    var l_Final_Str = ""
    val l_Sql = "begin PR_OUPUT_SERVICES(?,?,?); end;"
    l_clob002 = null
    l_err_level = 0L
    try {
      logInfo("performClob:Prepare Call at:")
      var stmt = connection.prepareCall(l_Sql).unwrap(classOf[OracleCallableStatement])
      stmt.setString(1, pAction)
      setClobAsString(stmt, 2, pClob)
      logInfo("performClob:After setClobAsString at:")
      stmt.registerOutParameter(3, Types.CLOB)
      logInfo("performClob:Begin execute at:")

      stmt.execute
      logInfo("performClob:End execute at:")
      l_clob002 = stmt.getClob(3)
      logInfo("performClob:Got Clob at:")
      if (stmt != null) {
        stmt.close()
        stmt = null
      }
    } catch {
      case e: SQLException =>
        logInfo("performClob:my Json error:" + e + "~ at level:" + l_err_level)
    }
    logInfo("performClob:Before convertclobToString at:")
    val l_temp_str = StringUtils.convertclobToString(l_clob002)
    logInfo("performClob:After convertclobToString at:")
    l_Final_Str = l_temp_str
    l_Final_Str
  }


  //  @throws[Exception]
  //  def convertclobToString(data: Clob): String = {
  //    val sb = new StringBuilder
  //    try {
  //      val reader = data.getCharacterStream
  //      val br = new BufferedReader(reader)
  //      var b = 0
  //      while (-1 != (b = br.read)) sb.append(b.toChar)
  //      br.close()
  //    } catch {
  //      case e: SQLException =>
  //        logInfo("SQL. Could not convert CLOB to string" + e.toString)
  //        return ""
  //      case e: IOException =>
  //        logInfo("IO. Could not convert CLOB to string" + e.toString)
  //        return ""
  //    }
  //    sb.toString
  //  }

  @throws[SQLException]
  def setClobAsString(ps: OracleCallableStatement, paramIndex: Int, content: String): Unit = {
    if (content != null) ps.setClob(paramIndex, new StringReader(content), content.length)
    else ps.setClob(paramIndex, null.asInstanceOf[Clob])
  }
}
