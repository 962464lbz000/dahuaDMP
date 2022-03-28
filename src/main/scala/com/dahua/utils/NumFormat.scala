package com.dahua.utils

object NumFormat {

  def toInt(field:String)={
    try{
      field.toInt
    }catch{
      case _:Exception => 0
    }
  }

  def toDouble(field:String)={
    try{
      field.toDouble
    }catch{
      case _:Exception => 0
    }
  }

}
