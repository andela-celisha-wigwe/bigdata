import org.apache.spark.sql.{SQLContext, Row, DataFrame, ColumnName}
import org.apache.spark.sql.functions.{count, lit, desc, sum}



// val hypothesis = udf[
// 	Double, // return
// 	Double, Double, Double,
// 	Double, Double, Double, Double
// ]((	count:Double, blacklist_count:Double, num_of_links:Double,
// 	C0:Double, C1:Double, C2:Double, C3:Double) => {
// 	C0 + (C1 * count) + (C2 * blacklist_count) + (C3 * num_of_links)
// })

// val error = udf[Double, Boolean, Double]((y: Boolean, h: Double) => {
// 	// we are using 1 and 0 for classification problem. 1 is for yes spam, 0 is for no spam
// 	val output = if(y) 1 else 0
// 	output - h
// })

// def deriv(dataFrame:DataFrame, C0:Double, C1:Double, C2:Double, C3:Double, j:Int) : Double = {
// 	val x = j match {
// 		case 0 => lit(1)
// 		case 1 => dataFrame("count")
// 		case 2 => dataFrame("blacklist_count")
// 		case 3 => dataFrame("num_of_links")
// 	}
// 	val hypValue = hypothesis(
// 		$"count", $"blacklist_count", $"num_of_links",
// 		lit(C0), lit(C1), lit(C2), lit(C3)
// 	)
// 	val errValue = error($"spam", hypValue)
// 	val sumDf = dataFrame.agg(sum(x * errValue))

// 	(1.0 / dataFrame.count()) * (sumDf.first.getDouble(0))
// }

// why can't I reference the DataFrame class that was inported.
class LinearReg(dataFrame: DataFrame) {

	// def normalize(value: Int, min: Int, max: Int) : Double = {
	// 	(value - min) / (max - min).toDouble
	// }

	// def getMinMax(colname: String) : (Long, Long)= {
	// 	dataFrame.agg(functions.min(colname), functions.max(colname)).first match {
	// 		case Row(x:Int, y:Int) => (x, y)
	// 		case Row(x:Long, y:Long) => (x, y)
	// 	}
	// }


	// def gradientDescent (dataset:DataFrame, alpha:Double) : (Double, Double, Double, Double) = {

	// 	var C0 = 0.0
	// 	var C1 = 0.0
	// 	var C2 = 0.0
	// 	var C3 = 0.0

	// 	var prevC0 = -1.0
	// 	var prevC1 = -1.0
	// 	var prevC2 = -1.0
	// 	var prevC3 = -1.0

	// 	var temp0 = 0.0
	// 	var temp1 = 0.0
	// 	var temp2 = 0.0
	// 	var temp3 = 0.0

	// 	var count = 0;

	// 	while (C0 != prevC0 && C1 != prevC1 && C2 != prevC2 && C3 != prevC3) {
	// 		temp0 = C0 - (alpha * deriv(dataFrame, C0, C1, C2, C3, 0))
	// 		temp1 = C1 - (alpha * deriv(dataFrame, C0, C1, C2, C3, 1))
	// 		temp2 = C2 - (alpha * deriv(dataFrame, C0, C1, C2, C3, 2))
	// 		temp3 = C3 - (alpha * deriv(dataFrame, C0, C1, C2, C3, 3))

	// 		prevC0 = C0
	// 		prevC1 = C1
	// 		prevC2 = C2
	// 		prevC3 = C3

	// 		C0 = temp0
	// 		C1 = temp1
	// 		C2 = temp2
	// 		C3 = temp3

	// 		println(C0, C1, C2, C3, count, "<========debugging the return values")
	// 		count += 1
	// 	}
	// 	(C0, C1, C2, C3) // return the parameters
	// }
}


// val blacklist = List("viagra", "pharmacy", "buy", 
// 	"email", "credit", "report", "free", "money",
// 	"programm", "business", "internet", "work", "financial",
// 	"mail", "million", "emails", "email", "call", "phone", "card",
// 	"reports", "marketing", "cost", "opportunity", "income", "cash",
// 	"easy", "thousands", "mailing", "programs", "simply", "buy", "sell")

// val blacklistAuthors = List("Anonymous")
// val sqlContext = new SQLContext(sc)
// // val data = sqlContext.jsonFile("/Users/elchroy/Roy/Code/bigdata/spiders/results.json").withColumn("spam", lit(false)).drop($"_corrupt_record").cache()
// val output = sqlContext.jsonFile("/Users/elchroy/Desktop/2001/output.json").withColumn("spam", lit(true)).cache()

// // count the number of http links in the conten
// val countHttp = udf[Int, String]((content:String) => ("http".r.findAllIn(content).toList.length))

// val countBlackList = udf[Int, String]((content:String) => {
// 	blacklist.foldLeft(0) ((acc, key) => acc + key.r.findAllIn(content).toList.length)
// })
// val countBlackListAuthors = udf[Boolean, String]((content:String) => (blacklist contains content))

// val dataFrame = (output
// 	// .unionAll(output) // merge both sure spam and unsure spam mails
// 	.filter("comment_content is not null") // take only not null comments
// 	.select("author", "comment_content", "spam") // select only the three columns
// 	.withColumn("blacklist_count", countBlackList($"comment_content"))
// 	.withColumn("is_anonymous", countBlackListAuthors($"author"))
// 	.withColumn("num_of_links", countHttp($"comment_content"))
// 	.groupBy($"comment_content", $"author", $"spam", $"is_anonymous", $"num_of_links", $"blacklist_count")
// 	.agg(count(lit(1)).as("count"))
// 	.sort(desc("num_of_links"), desc("count"), desc("blacklist_count"))
// ).cache()

// val lr = new LinearReg(dataFrame)

// def normalizer (colmin:Long, colmax:Long) = () => udf[Double, Int]((value: Int) => ((value - colmin) / (colmax - colmin).toDouble))

// val (numOfLinksMin, numOfLinksMax) = lr getMinMax "num_of_links"
// val numLinkNormalizer = normalizer(numOfLinksMin, numOfLinksMax)()

// val (countMin, countMax) = lr getMinMax "count"
// val countNormalizer = normalizer(countMin, countMax)()

// val (blacklistCountMin, blacklistCountMax) = lr getMinMax "blacklist_count"
// val blackListNormalizer = normalizer(blacklistCountMin, blacklistCountMax)()


// val spamFactor = udf[Double, Double, Double, Double]((numLinks: Double, count: Double, blacklistCount: Double) => {
// 	(numLinks + count + blacklistCount * 2) / 4
// })

// val limit = 0.006015550886152264

// val realFinalUDF = udf[Boolean, Double]((spam_factor: Double) => (spam_factor >= limit))

// val normalizedDF = (dataFrame
// 	.withColumn("blacklist_count", blackListNormalizer($"blacklist_count"))
// 	.withColumn("num_of_links", numLinkNormalizer($"num_of_links"))
// 	.withColumn("count", countNormalizer($"count"))

// 	.withColumn("spam_factor", spamFactor($"num_of_links", $"count", $"blacklist_count"))
// 	.sort(desc("spam_factor"))
// 	.withColumn("spam", realFinalUDF($"spam_factor"))
// ).cache()



// val (C0, C1, C2, C3) = lr.gradientDescent(normalizedDF, 0.03)

// // after 163 iterations, these are the values of the parameters
// // (-4.336851292006015E297,-6.444718645382433E300,-2.097017038431169E298,-5.743423302898537E297)
// // (C0, C1, C2, C3) = (-4.3368512920060166E297,-6.444718645382444E300,-2.097017038431174E298,-5.74342330289856E297)

// // val limit = normalizedDF.stat.approxQuantile("spam_factor", Array(0.98), 0).last
// // val realFinalDF = normalizedDF.withColumn("spam", realFinalUDF($"spam_factor"))
