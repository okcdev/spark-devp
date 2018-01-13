package com.robinson.spark.mllib

import com.alex.spark.util.SparkEnv
import com.robinson.spark.util.SparkEnv
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by fengtao.xue on 2017/12/07.
  */
object ClassifyRegression {

  def LogistReg():Unit= {

    //load spam and ham emails from text files
    val spam = SparkEnv.sc.textFile("data/span.txt")
    val ham = SparkEnv.sc.textFile("data/ham.txt")

    //create a hashingTF instance to map email text to vectors of 100 features
    val tf = new HashingTF(numFeatures = 100)
    //each email is split into words, and each word is mapped to one feature.
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val hamFeatures = ham.map(email => tf.transform(email.split(" ")))

    // create LabeledPoint datasets for positive (spam) and negative (ham) examples.
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = hamFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples ++ negativeExamples
    trainingData.cache() // Cache data since Logistic Regression is an iterative algorithm.

    //create a logistic regression learner which uses the LBFGS optimizer.
    val lrLearner = new LogisticRegressionWithSGD()
    //Run the actual learning algorithm on the training data
    val model = lrLearner.run(trainingData)

    // Test on a positive example (spam) and a negative one (ham).
    // First apply the same HashingTF feature transformation used on the training data.
    val posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
    // Now use the learned model to predict spam/ham for new emails.
    println(s"Prediction for positive test example: ${model.predict(posTestExample)}")
    println(s"Prediction for negative test example: ${model.predict(negTestExample)}")

  }
}
