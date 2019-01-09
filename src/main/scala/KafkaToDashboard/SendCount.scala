package KafkaToDashboard

import scalaj.http.Http

object SendCount {

  def SendCount(namo_data: String, raga_data: String ): Unit = {

    try{
      Http("https://20190109t010254-dot-default-dot-spartan-alcove-224117.appspot.com/updateData").
        postForm(Seq("label" -> "[1,2,3,4,5,6,7,8,9,10]",
          "namo_count" -> namo_data, "raga_count" -> raga_data)
        ).asString

    }catch{
      case e: Exception =>
        println("Unable to send count data to dashboard")
    }
  }

  def SendSentimentCount(namo_data_positive: String, namo_data_negative: String,raga_data_positive: String, raga_data_negative: String ): Unit = {

    try{
      Http("https://20190109t010254-dot-default-dot-spartan-alcove-224117.appspot.com/updateSentimentData").
        postForm(Seq("label" -> "[1,2,3,4,5,6,7,8,9,10]",
          "namo_positive" -> namo_data_positive, "raga_positive" -> raga_data_positive,
          "namo_negative" -> namo_data_negative, "raga_negative" -> raga_data_negative)
        ).asString

    }catch{
      case e: Exception =>
        println("Unable to send sentiment data to dashboard")
    }
  }

}
