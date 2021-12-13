package producer

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

object Api {

    /**
      * Description: Object that makes calls to the Mockaroo API for the different topics.
      * User must have a valid Mockaroo key and be added to the proper group in order to
      * access these API calls. User should also provide key to apiKeyArray in order to 
      * reduce the likelihood of exceeding API call limits. This object is packaged and loaded
      * into the Kafka.scala file during execution.
      * @param url: the Mockaroo URL to be called in exchange for JSON-formatted data.
      * @return content: JSON formatted data resulting from API call.
    */

    def getRestContent(url: String): String = {
        /**
         * Description: Function that makes call to online API and receives JSON-formatted string.
         * String is then modified to remove brackets and braces for processing into subsequent functions.
         * @param url: the Mockaroo URL to be called in exchange for JSON-formatted data.
         * @return content: JSON formatted data resulting from API call.
        */

        // Connect to the Internet of Things
        val httpClient = new DefaultHttpClient()
        val httpResponse = httpClient.execute(new HttpGet(url))
        val entity = httpResponse.getEntity()
        

        var content = ""
        if (entity != null) {
            val inputStream = entity.getContent() //Receive JSON string

            // Modify string to remove extraneous brackets.
            content = scala.io.Source.fromInputStream(inputStream)
                        .getLines
                        .mkString("")
                        .replace("},", "}#")
                        .replace("[", "")
                        .replace("]", "")          // hopefully no hierarchies in the schemas <- This could be incorporated as a stretch goal.
            inputStream.close          

        }       
        httpClient.getConnectionManager().shutdown() // Close connection to IoT  
        content
    }

    // New users should add their unique API key to this array.
    final val apiKeyArray = Array("2f9538c0", "235571b0", "fe859db0", "d7f87c40")
    var keyIter = 0
    
    //Calls to Mockaroo API to generate mock data for different topics.
    val recruiterURL        =   "https://my.api.mockaroo.com/Recruiters?key="
    val qlDataURL           =   "https://my.api.mockaroo.com/Qualified_Lead?key="
    val screenerDataURL     =   "https://my.api.mockaroo.com/Screeners?key="
    val offerDataURL        =   "https://my.api.mockaroo.com/Offers?key="
    val screeningDataURL    =   "https://my.api.mockaroo.com/Screening?key="
    val caDataURL           =   "https://my.api.mockaroo.com/Contact_Attempts?key="
    val qlBIGDataURL        =   "https://my.api.mockaroo.com/Qualified_Lead_Big_Table?key="

    var keyCounter = 0;

    def obtainData(url: String): Array[String] = {
        /**
         * Description: Function that iterates through the apiKeyArray and calls getRestContent() 
         * for a specific URL. If the JSON string contains the keyword "error", the next key will be
         * used, and the API call will be attempted again. If successful, string will be formatted.
         * @param url: the Mockaroo URL to be called in exchange for JSON-formatted data.
         * @return content: JSON string that is formatted for further processing in subsequent programs.
        */
        var exit = false
        var JSON = Array[String]();
        var key = "";

        while(!exit && keyCounter < apiKeyArray.length){ 
            try{
                key = apiKeyArray(keyCounter);
                //println(key)
                JSON = getRestContent(url+key).split("#")
                if(JSON(0).contains("error")!=true){
                    exit = true
                }else{
                    println(s"Invalid key $key")
                    keyCounter += 1;
                }
            }catch{
                case ex : Throwable => {
                    ex.printStackTrace();
                    println(s"API was not called because call limit for $key has been met")
                    throw new Exception (s"${ex.getMessage}")
                }
            }
        }

        if(keyCounter == apiKeyArray.length) // Reset
            keyCounter = 0; 
        JSON
    }


    // The following 7 functions serve the same purpose, except for calling different URL string
    // representing the different topics.

    def recruiterData(): Array[String] =   obtainData(recruiterURL)

    def qlData(): Array[String] =   obtainData(qlDataURL)

    def screenerData(): Array[String] =   obtainData(screenerDataURL)

    def offerData(): Array[String] =   obtainData(offerDataURL)

    def screeningData(): Array[String] =   obtainData(screeningDataURL)

    def caData(): Array[String] =   obtainData(caDataURL)

    def qlBIGData(): Array[String] =   obtainData(qlBIGDataURL)

}
}
