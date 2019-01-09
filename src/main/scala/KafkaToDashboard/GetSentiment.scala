package KafkaToDashboard

import play.api.libs.json._
import scalaj.http._

object GetSentiment {

  def GetSentiment(sentence: String): Int = {

    val sentenceToPredict = "{\"inputs\":{\"sentence\": \"" + sentence.toLowerCase.replaceAll("\\W", "") + "\"}}"
    val response1 = Http("http://34.73.172.57:8501/v1/models/lstm:predict").postData(sentenceToPredict).asString.body

    val JsValue_json_string: JsValue = Json.parse(response1)
    val sentiment: Int = (JsValue_json_string \ "outputs" \ "classes" \ 0).as[Int]

    return  sentiment
  }

}
