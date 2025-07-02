// Mock que evita llamadas a Hadoop
  when(spark.sparkContext.hadoopConfiguration).thenReturn(new org.apache.hadoop.conf.Configuration {
    override def get(key: String): String = null
  })
