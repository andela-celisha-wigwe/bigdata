import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{count, lit, desc}

val blacklist = List("viagra", "pharmacy", "buy")
val blacklistAuthors = List("Anonymous")
val sqlContext = new SQLContext(sc)
// sc is a variable set by spark-shell
// of type SparkContext

val data = sqlContext.jsonFile("/Users/elchroy/Roy/Code/bigdata/spiders/results.json")

val countHttp = udf[Int, String]((content:String) => "http".r.findAllIn(content).toList.length)

// normalize function 
val countBlackList = udf[Int, String]((content: String) => (
	blacklist.foldLeft(0) ((acc, key) => {
		acc + key.r.findAllIn(content).toList.length
	})
))

val countBlackListAuthors = udf[Boolean, String]((content: String) => (
	blacklistAuthors.contains(content)
))

val newDF = data.filter("comment_content is not null").select("author", "comment_content").withColumn("blacklist_count", countBlackList($"comment_content")).withColumn("is_anonymous", countBlackListAuthors($"author")).withColumn("num_of_links", countHttp($"comment_content")).groupBy($"comment_content", $"author", $"is_anonymous", $"num_of_links", $"blacklist_count").agg(count(lit(1)).as("count"))

val sortedDF = newDF.sort(desc("num_of_links"), desc("count"), desc("blacklist_count"))

val (numOfLinksMin, numOfLinksMax) = sortedDF.agg(min($"num_of_links"), max($"num_of_links")).first match {
  case Row(x: Int, y: Int) => (x, y)
}

val (countMin, countMax) = sortedDF.agg(min($"count"), max($"count")).first match {
  case Row(x: Int, y: Int) => (x, y)
}

val (blacklistCountMin, blacklistCountMax) = sortedDF.agg(min($"blacklist_count"), max($"blacklist_count")).first match {
  case Row(x: Int, y: Int) => (x, y)
}

// val normalize = udf[Double, Int, Int, Int]((value: Int, min: Int, max: Int) => {
// 	(value - min) / (max - min)
// })

val normalizeNumLinks = udf[Double, Int]((value: Int) => {
	(value - numOfLinksMin) / (numOfLinksMax - numOfLinksMin)
})

// val normalizeCount = udf[Double, Int]((value: Int) => {
// 	(value - countMin) / (countMax - countMin)
// })

val normalizeBlackListCount = udf[Double, Int]((value: Int) => {
	(value - blacklistCountMin) / (blacklistCountMax - blacklistCountMin)
})

/// this helped us see the actual error becuase the scala was eevaluating in another thread.
// println(countMin, countMax)

val normalizedDF = sortedDF.withColumn("blacklist_count", normalizeBlackListCount($"blacklist_count")).withColumn("num_of_links", normalizeNumLinks($"num_of_links"))//.withColumn("count", normalizeCount($"count"))

// def sigma(numLinks: Int, count: Int, blacklistCount: Int) : Double = {
// 	// return a number between 0 and 1.
// 	1.0
// }



