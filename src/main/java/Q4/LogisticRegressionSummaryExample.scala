package Q4

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{Imputer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max, regexp_extract, udf}

object LogisticRegressionSummaryExample {

  def balanceDataset(dataset: DataFrame): DataFrame = {

    val numPositives = dataset.filter(dataset("label") === 1).count
    val datasetSize = dataset.count
    val balancingRatio = (datasetSize - numPositives).toDouble / datasetSize

    val calculateWeights = udf { d: Double =>
      if (d == 1.0) {
        1 * balancingRatio
      }
      else {
        (1 * (1.0 - balancingRatio))
      }
    }

    val weightedDataset = dataset.withColumn("classWeightCol", calculateWeights(dataset("label")))
    weightedDataset
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LogisticRegressionSummaryExample")
      .getOrCreate()
    import spark.implicits._

    val raw = spark.read.option("header", "true").format("csv").load(args(0))
    val temp = raw.withColumnRenamed("is_default","label")
    val temp2 = temp.drop("loan_id","user_id","initial_list_status", "early_return","earlies_credit_line","early_return_amount","early_return_amount_3mon", "policy_code","title","initial_list_status","earlies_credit_mon","pub_dero_bankrup", "scoring_high","post_code","issue_date","censor_status","sub_class")
    val indexer_wk = new StringIndexer().setInputCol("work_type").setOutputCol("work_type_index")
    val indexed_wk = indexer_wk.fit(temp2).transform(temp2).drop("work_type")
    val indexer_cl = new StringIndexer().setInputCol("class").setOutputCol("class_index")
    val indexed_cl = indexer_cl.fit(indexed_wk).transform(indexed_wk).drop("class")
    val indexer_et = new StringIndexer().setInputCol("employer_type").setOutputCol("employer_type_index")
    val indexed_et = indexer_et.fit(indexed_cl).transform(indexed_cl).drop("employer_type")
    val indexer_ind = new StringIndexer().setInputCol("industry").setOutputCol("industry_index")
    val indexed_ind = indexer_ind.fit(indexed_et).transform(indexed_et).drop("industry")

    val indexed_wy = indexed_ind.withColumn("work_year_extract",regexp_extract(col("work_year"),"[0-9]+",0)).drop("work_year")

    val castedDF = indexed_wy.columns.foldLeft(indexed_wy)((current, c) => current.withColumn(c, col(c).cast("float")))

    val imputer = new Imputer().setInputCols(castedDF.columns).setOutputCols(castedDF.columns.map(c => s"${c}_imputed")).setStrategy("mode")

    val fin = imputer.fit(castedDF).transform(castedDF)

    val fin_drop = fin.drop("total_loan","year_of_loan","interest","monthly_payment","house_exist","house_loan_status","marriage","offsprings","use","region","debt_loan_ratio","del_in_18month","scoring_low","recircle_b","recircle_u","f0","f1","f2","f3","f4","f5","label_imputed","work_type_index","class_index","employer_type_index","industry_index","work_year_extract")

    val training = balanceDataset(fin_drop)
    val assembler = new VectorAssembler().setInputCols(Array("total_loan_imputed","year_of_loan_imputed","interest_imputed","monthly_payment_imputed","house_exist_imputed","house_loan_status_imputed","marriage_imputed","offsprings_imputed","use_imputed","region_imputed","debt_loan_ratio_imputed","del_in_18month_imputed","scoring_low_imputed","recircle_b_imputed","recircle_u_imputed","f0_imputed","f1_imputed","f2_imputed","f3_imputed","f4_imputed","f5_imputed","work_type_index_imputed","class_index_imputed","employer_type_index_imputed","industry_index_imputed","work_year_extract_imputed")).setOutputCol("features")
    val output = assembler.transform(training).drop("total_loan_imputed","year_of_loan_imputed","interest_imputed","monthly_payment_imputed","house_exist_imputed","house_loan_status_imputed","marriage_imputed","offsprings_imputed","use_imputed","region_imputed","debt_loan_ratio_imputed","del_in_18month_imputed","scoring_low_imputed","recircle_b_imputed","recircle_u_imputed","f0_imputed","f1_imputed","f2_imputed","f3_imputed","f4_imputed","f5_imputed","work_type_index_imputed","class_index_imputed","employer_type_index_imputed","industry_index_imputed","work_year_extract_imputed")

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setWeightCol("classWeightCol").setLabelCol("label").setFeaturesCol("features")
    val lrModel = lr.fit(output)

    val trainingSummary = lrModel.binarySummary

    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

    val roc = trainingSummary.roc
    roc.show()
    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

    val fMeasure = trainingSummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).select("threshold").head().getDouble(0)
    println(s"maxFMeasure=${maxFMeasure}")
    println(s"bestThreshold=${bestThreshold}")
    lrModel.setThreshold(bestThreshold)

    spark.stop()
  }
}
