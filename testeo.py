private def fechasDeParticion(targetdb: String)(implicit spark: SparkSession): List[String] = {
  spark.sql(s"show partitions $targetdb.${BDRUtils.starting_points_contract}")
    .select("partition")
    .collect()
    .map(_.getString(0))                    // "partition=2021-05-31"
    .flatMap(_.split("=", 2).lift(1))       // -> "2021-05-31"
    .filter(_.matches("\\d{4}-\\d{2}-\\d{2}"))
    .toList
    .sorted
}
