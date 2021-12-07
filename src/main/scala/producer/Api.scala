package producer //going to be producer when added to main project

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

object Api {


    var tmpStr1 = """ id: 0, name: jj, lname: mer"""
    var tmpStr = tmpStr1 + "\n" +tmpStr1 + "\n" +tmpStr1+"\n" + tmpStr1;
    

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

   
    final val apiKeyArray = Array("jksgn32","2f9538c0", "235571b0", "fe859db0", "d7f87c40")
    var keyIter = 0
    var invalidKeyList = Array()
    //Calls to Mockaroo API to generate mock data
    
    // Iterate through apiKeyArray until first key that produces a message

    // def recruiterData(): Array[String] =    
    //     for(key <- 0 to apiKeyArray.length){
    //         try{
    //             getRestContent(s"https://my.api.mockaroo.com/Recruiters?key=$key").split("},")
    //         }catch{
    //             case ex : Throwable => {
    //             ex.printStackTrace();
    //             println(s"API was not called because call limit for $key has been met")
    //             throw new Exception (s"${ex.getMessage}")
    //             }
    //         }
    //     }
    
    val RecruiterURL        =   "https://my.api.mockaroo.com/Recruiters?key="
    val qlDataURL           =   "https://my.api.mockaroo.com/Qualified_Lead?key="
    val screenerDataURL     =   "https://my.api.mockaroo.com/Screeners?key="
    val offerDataURL        =   "https://my.api.mockaroo.com/Offers?key="
    val screeningDataURL    =   "https://my.api.mockaroo.com/Screening?key="
    val caDataURL           =   "https://my.api.mockaroo.com/Contact_Attempts?key="
    val qlBIGDataURL        =   "https://my.api.mockaroo.com/Qualified_Lead_Big_Table?key="

    var keyCounter = 0;

    def obtainData(url: String): Array[String] = {
        var exit = false
        var JSON = Array[String]();
        var key = "";

        while(!exit && keyCounter < apiKeyArray.length){ 
            // println("Starting the while loop") //delete this when finished
            try{
                key = apiKeyArray(keyCounter);
                //println(key)
                JSON = getRestContent(url+key).split("},")
                if(JSON(0).contains("error")!=true){
                    exit = true
                }else{
                    println(s"Invalid key $key") //delete this when finished
                    keyCounter += 1; // update the key counter by 1
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
        //println("while loop stopped") //delete this when finished
        JSON
    }

    def recruiterData(): Array[String] =   obtainData(RecruiterURL)

    def qlData(): Array[String] =   obtainData(qlDataURL)

    def screenerData(): Array[String] =   obtainData(screenerDataURL)

    def offerData(): Array[String] =   obtainData(offerDataURL)

    def screeningData(): Array[String] =   obtainData(screeningDataURL)

    def caData(): Array[String] =   obtainData(caDataURL)

    def qlBIGData(): Array[String] =   obtainData(qlBIGDataURL)

    //def recruiterData(): Array[String] =    getRestContent(s"https://my.api.mockaroo.com/Recruiters?key=${apiKeyArray(2)}").split("},")

    // def qlData(): Array[String] =           getRestContent(s"https://my.api.mockaroo.com/Qualified_Lead?key=${apiKeyArray(2)}").split("},")

    // def screenerData(): Array[String] =     getRestContent(s"https://my.api.mockaroo.com/Screeners?key=${apiKeyArray(2)}").split("},")

    // def offerData(): Array[String] =        getRestContent(s"https://my.api.mockaroo.com/Offers?key=${apiKeyArray(2)}").split("},")

    // def screeningData(): Array[String] =    getRestContent(s"https://my.api.mockaroo.com/Screening?key=${apiKeyArray(2)}").split("},")

    // def caData(): Array[String] =           getRestContent(s"https://my.api.mockaroo.com/Contact_Attempts?key=${apiKeyArray(2)}").split("},")

    // def qlBIGData(): Array[String] =        getRestContent(s"https://my.api.mockaroo.com/Qualified_Lead_Big_Table?key=${apiKeyArray(2)}").split("},")

//////
    // def recruiterData(): Array[String] =    tmpStr.split("\n");

    // def qlData(): Array[String] =           tmpStr.split("\n");

    // def screenerData(): Array[String] =     tmpStr.split("\n");

    // def offerData(): Array[String] =        tmpStr.split("\n");

    // def screeningData(): Array[String] =    tmpStr.split("\n");

    // def caData(): Array[String] =           tmpStr.split("\n");

    // def qlBIGData(): Array[String] =        tmpStr.split("\n");

}