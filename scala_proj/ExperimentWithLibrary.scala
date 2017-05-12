import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{count, lit, desc, sum}
import org.apache.spark.sql.DataFrame

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.MulticlassMetrics

val blacklist = List("viagra", "pharmacy", "buy", 
	"email", "credit", "report", "free", "money",
	"programm", "business", "internet", "work", "financial",
	"mail", "million", "emails", "email", "call", "phone", "card",
	"reports", "marketing", "cost", "opportunity", "income", "cash",
	"easy", "thousands", "mailing", "programs", "simply", "buy", "sell")

val blacklistAuthors = List("Anonymous")
val sqlContext = new SQLContext(sc)
// sc is a variable set by spark-shell
// of type SparkContext


val data = sqlContext.jsonFile("/Users/elchroy/Roy/Code/bigdata/spiders/results.json").withColumn("spam", lit(false)).drop($"_corrupt_record")
val output = sqlContext.jsonFile("/Users/elchroy/Desktop/2001/output.json").withColumn("spam", lit(true))

val mergedDF = data.unionAll(output)

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

val newDF = (mergedDF
	.filter("comment_content is not null")
	.select("author", "comment_content", "spam")
	.withColumn("blacklist_count", countBlackList($"comment_content"))
	.withColumn("is_anonymous", countBlackListAuthors($"author"))
	.withColumn("num_of_links", countHttp($"comment_content"))
	.groupBy($"comment_content", $"author", $"spam", $"is_anonymous", $"num_of_links", $"blacklist_count")
	.agg(count(lit(1)).as("count"))
)

val sortedDF = newDF.sort(desc("num_of_links"), desc("count"), desc("blacklist_count")).cache() // cache he
// 

val (numOfLinksMin, numOfLinksMax) = sortedDF.agg(min($"num_of_links"), max($"num_of_links")).first match {
  case Row(x: Int, y: Int) => (x, y)
}

val (countMin, countMax) = sortedDF.agg(min($"count"), max($"count")).first match {
  case Row(x: Long, y: Long) => (x, y)
}

val (blacklistCountMin, blacklistCountMax) = sortedDF.agg(min($"blacklist_count"), max($"blacklist_count")).first match {
  case Row(x: Int, y: Int) => (x, y)
}

// val normalize = udf[Double, Int, Int, Int]((value: Int, min: Int, max: Int) => {
// 	(value - min) / (max - min)
// })

val normalizeNumLinks = udf[Double, Int]((value: Int) => {
	(value - numOfLinksMin) / (numOfLinksMax - numOfLinksMin).toDouble
})

val normalizeCount = udf[Double, Int]((value: Int) => {
	(value - countMin) / (countMax - countMin).toDouble
})

val normalizeBlackListCount = udf[Double, Int]((value: Int) => {
	(value - blacklistCountMin) / (blacklistCountMax - blacklistCountMin).toDouble
})

val normalizedDF = sortedDF.withColumn("blacklist_count", normalizeBlackListCount($"blacklist_count")).withColumn("num_of_links", normalizeNumLinks($"num_of_links")).withColumn("count", normalizeCount($"count"))

def sigma(numLinks: Double, count: Double, blacklistCount: Double) : Double = {
	(numLinks + count + blacklistCount * 2) / 4
}

val spamFactor = udf[Double, Double, Double, Double](sigma)
val finalDF = normalizedDF.withColumn("spam_factor", spamFactor($"num_of_links", $"count", $"blacklist_count")).sort(desc("spam_factor"))
val limit = finalDF.stat.approxQuantile("spam_factor", Array(0.98), 0).last
val realFinalUDF = udf[Int, Double, Boolean]((spam_factor: Double, spam: Boolean) => {
	if (spam || spam_factor >= limit) 1 else 0
})
val realFinalDF = finalDF.withColumn("label", realFinalUDF($"spam_factor", $"spam")).cache()
// the pipelineexpects the dataframe to have a column called label that would be expected value of the model.

realFinalDF.show(20)

val assembler = (new VectorAssembler()
	.setInputCols(Array("num_of_links", "blacklist_count", "count"))
	.setOutputCol("features")
)

val Array(trainingData, testData) = realFinalDF.randomSplit(Array(0.8, 0.2), seed = System.currentTimeMillis)
val lr = new LogisticRegression()

// this is a pipeline
// The first statge is the pipe the dataset throught the assembler
// The second stage is to pipe the databset thought the logistic regression object
val pipeline = new Pipeline().setStages(Array(assembler, lr))

// grab the trainingData, pass throught the pipeline
// the result is the training model => C0 ... C4 features.
val model = pipeline.fit(trainingData)

// Grab all testdata and pipe transform according to the created model.
val results = model.transform(testData)

val predictionAndLabels = (results
	.select($"prediction", $"label") // selecting prediction and label
	.as[(Double, Double)] // we want th preditcion andn label to be returned as two Doubles in a tuple
	.rdd // we want the rdd of the dataframe -> rdd : 
)

val metrics = new MulticlassMetrics(predictionAndLabels)

println(metrics.confusionMatrix) // this is a matrix that epresses the percentage of correctness in the model
// the confuasion matrix is always a 2 X 2 matrix, such that
// 			a11 => true positive (we got it as spam)										a12 => false positive (we failed it by declaring as spam)
// 			a21 => false negative (we failed it by declaring as not spam)					a22 => true negative (we got it as not spam)






