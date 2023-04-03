package com.msb.utils

import org.slf4j.{Logger, LoggerFactory}

import java.io.Serializable

trait Logging extends Serializable {

  private lazy val logger: Logger = initLog()

  private def loggerName: String =
    this.getClass.getName.stripSuffix("$")

  private def initLog(): Logger = {
    LoggerFactory.getLogger(loggerName)
  }

  def logInfo(msg: => String): Unit = {
    if (logger.isInfoEnabled) {
      logger.info(msg)
    }
  }

  def logDebug(msg: => String): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(msg)
    }
  }

  def logTrace(msg: => String): Unit = {
    if (logger.isTraceEnabled) {
      logger.trace(msg)
    }
  }


  def logError(msg: => String): Unit = {
    if (logger.isErrorEnabled) {
      logger.error(msg)
    }
  }
}
