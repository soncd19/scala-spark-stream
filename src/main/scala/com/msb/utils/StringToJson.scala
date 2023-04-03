package com.msb.utils

import com.google.gson.{JsonObject, JsonParser}

object StringToJson {
  def toJson(data: String): JsonObject = {
    new JsonParser().parse(data).getAsJsonObject
  }
}
