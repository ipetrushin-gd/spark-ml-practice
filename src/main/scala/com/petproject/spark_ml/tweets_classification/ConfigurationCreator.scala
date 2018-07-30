package com.petproject.spark_ml.tweets_classification

import com.typesafe.config.ConfigFactory

trait ConfigurationCreator {
  lazy val config = ConfigFactory.parseResources("application.conf")
}
