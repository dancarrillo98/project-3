package example

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

object Api {

    /**
      * 
      *
      * @param url
      * @return content
      */
    def getRestContent(url: String): String = {

        val httpClient = new DefaultHttpClient()
        val httpResponse = httpClient.execute(new HttpGet(url))
        val entity = httpResponse.getEntity()
        
        var content = ""
        if (entity != null) {
            val inputStream = entity.getContent()
            content = scala.io.Source.fromInputStream(inputStream)
                        .getLines
                        .mkString("")
                        .replace("{", "")
                        .replace("[", "")
                        .replace("}]", "")          // hopefully no hierarchies in the schemas
            inputStream.close           
        }       

        httpClient.getConnectionManager().shutdown()
        
        content
    }

   
    final val apiKey = "2f9538c0"

    // Calls to Mockaroo API to generate mock data
    
    def recruiterData(): Array[String] =    getRestContent(s"https://my.api.mockaroo.com/Recruiters?key=$apiKey").split("},")

    def qlData(): Array[String] =           getRestContent(s"https://my.api.mockaroo.com/Qualified_Lead?key=$apiKey").split("},")

    def screenerData(): Array[String] =     getRestContent(s"https://my.api.mockaroo.com/Screeners?key=$apiKey").split("},")

    def offerData(): Array[String] =        getRestContent(s"https://my.api.mockaroo.com/Offers?key=$apiKey").split("},")

    def screeningData(): Array[String] =    getRestContent(s"https://my.api.mockaroo.com/Screening?key=$apiKey").split("},")

    def caData(): Array[String] =           getRestContent(s"https://my.api.mockaroo.com/Contact_Attempts?key=$apiKey").split("},")

    def qlBIGData(): Array[String] =        getRestContent(s"https://my.api.mockaroo.com/Qualified_Lead_Big_Table?key=$apiKey").split("},")

}