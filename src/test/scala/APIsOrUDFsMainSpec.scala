import com.bla.bla.features.ClientsFeatures
import com.bla.bla.utils.Models.Client
import com.bla.bla.utils.{Models, SparkSession}
import org.scalatest._
import org.apache.spark.sql.functions._
import SparkSession._

class APIsOrUDFsMainSpec extends FlatSpec with Matchers {

  /**
    * The idea here is to transform the dataframe thanks to a function provided by the API
    *
    * Pros:
    *   Probably more optimized
    *   Less serialization and network traffic
    *
    * Cons:
    *   A lot harder to test
    */

  "tagAdultClientsAPI" should "add a column with the right adult tag" in {
    val clientListDF = sqlContext.createDataFrame(
      List(
        Client(1, "Menestret", "Martin", 27),
        Client(1, "Dubus", "Georges", 17))
    )

    val result = ClientsFeatures.tagAdultClientsDF(clientListDF)

    result.columns.contains("isAdult") shouldBe true
    result.filter((col("age") <= 18) && col("isAdult") === true).count() should equal(0)
    result.filter((col("age") <= 18) && col("isAdult") === false).count() should equal(1)
    result.filter((col("age") > 18) && col("isAdult") === true).count() should equal(1)

    sc.stop()
  }

  /**
    * The idea here is to transform the dataframe thanks to a BUSINESS function injected in an UDF
    *
    * Pros:
    *   A lot easier to test
    *   Able to deal with business functions all along and couple with Spark only when needed
    */

  "isAdult" should "return the correct adultTag corresponding to the client age" in {
    ClientsFeatures.isAdult(1) shouldBe false
    ClientsFeatures.isAdult(18) shouldBe true
    ClientsFeatures.isAdult(31) shouldBe true
  }
}
