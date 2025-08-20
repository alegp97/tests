def getDfRestoVariables: DataFrame = {

  // Sonarqube fixed cognitive complexity: condiciones agrupadas en listas y plegadas con reduceOption/foldRight para evitar cadenas largas de || y when/otherwise

  val name    = col("variable_name")
  val country = col("country")

  val like: String => Column = pat => name.like(pat)
  val orAll: Seq[Column] => Column = _.reduceOption(_ || _).getOrElse(lit(false))
  val chainWhen: (Seq[(Column, Column)], Column) => Column =
    (pairs, defaultV) => pairs.foldRight(defaultV){ case ((c,v), acc) => when(c, v).otherwise(acc) }

  val timelineCond = {
    val parts = Seq(
      if (getYearCondition)   Some(col("timeline").like("%Q%")) else None,
      if (getAnnualCondition) Some(col("timeline") === "A")     else None
    ).flatten
    parts.reduceOption(_ || _).getOrElse(lit(true))
  }

  val interbankUSCAN =
    Seq(interbank_daily, interbank_weekly, interbank_monthly)
      .map(name.like)
      .map(_ && country.isin("US","CAN"))
      .reduce(_ || _)

  val variableAny = {
    val simples = Seq(
      "%GDP (% YOY)%", "%UNEMPLOYMENT (% ACTIVE POPULATION)%", "%CPI (% YOY)%",
      "%REAL ESTATE: HOUSING PRIC% (% YOY)%", "%REAL ESTATE: LAND PRIC% (% YOY)%",
      "%OFFICIALS%", "%KOS0R%",
      "%SOVEREIGN BONDS 2 YEARS%", "%SOVEREIGN BONDS 3 YEARS%",
      "%SOVEREIGN BONDS 5 YEARS%", "%SOVEREIGN BONDS 10 YEARS%",
      "%SOVEREIGN BOND SPREAD VS GERMANY (10Y, BP)%",
      "%SOVEREIGN BOND SPREAD VS USA (10Y, BP)%",
      "% EUR (END OF PERIOD)%", "%/ EUR (AVERAGE OF PERIOD)%"
    ).map(like) ++ Seq(
      name.like(inflation_uf),
      name.like(inflation_bonds),
      name.like(inflation_linked_bonds),
      name.like(inflation_linked_2),
      name.like(inflation_linked_5),
      name.like(inflation_linked_10),
      name.like(inflation_linked_20)
    )
    orAll(simples :+ interbankUSCAN)
  }

  val baseFilter =
    col("unit_id")          === unit_id         &&
    col("entity_id")        === entity_id       &&
    col("exercise")         === exercise        &&
    col("scenario_name")    === scenario_name   &&
    col("scenario_version") === scenario_version &&
    timelineCond && variableAny

  val restoVariables =
    SQLContext.table(s"$common_db.scenarios")
      .where(baseFilter)
      .select(
        chainWhen(
          Seq(
            (name.like(inflation_uf),                               lit("I. FINANCIAL MARKETS SCENARIO")),
            ((name.like(inflation_bonds) || name.like(inflation_linked_bonds)),
              lit("II. FINANCIAL MARKETS SCENARIO"))
          ),
          col("category")
        ).as("category"),

        chainWhen(
          Seq(
            (like("%GDP (% YOY)%"),                               lit(1)),
            (like("%UNEMPLOYMENT (% ACTIVE POPULATION)%"),        lit(2)),
            (like("%CPI (% YOY)%"),                                lit(3)),
            (like("%REAL ESTATE: HOUSING PRIC% (% YOY)%"),        lit(4)),
            (like("%REAL ESTATE: LAND PRIC% (% YOY)%"),           lit(5)),
            (like("%OFFICIALS%"),                                  lit(6)),
            (name.like(interbank_daily) || name.like(interbank_weekly) || name.like(interbank_monthly),
              when(country === "URU", lit(6)).otherwise(col("interbank_rates_abs"))),
            (name.like(inflation_bonds) || name.like(inflation_uf), lit(7)),
            (like("%KOS0R%"),                                      lit(8)),
            (like("%SOVEREIGN BONDS 2 YEARS%"),                    lit(9)),
            (like("%SOVEREIGN BONDS 3 YEARS%"),                    lit(10)),
            (like("%SOVEREIGN BONDS 5 YEARS%"),                    lit(11)),
            (like("%SOVEREIGN BONDS 10 YEARS%"),                   lit(12)),
            (like("%SOVEREIGN BOND SPREAD VS GERMANY (10Y, BP)%"), lit(13)),
            (like("%SOVEREIGN BOND SPREAD VS USA (10Y, BP)%"),     lit(14)),
            (like("% EUR (END OF PERIOD)%"),                       lit(24)),
            (like("%/ EUR (% OF PERIOD)%"),                        split(name, "\\W")(0))
          ),
          lit(1000)
        ).as("ordervariable"),

        chainWhen(
          Seq(
            (
              name.like(interbank_daily) || name.like(interbank_weekly) || name.like(interbank_monthly),
              concat(
                when(country === "UK",  lit("LIBOR"))
                  .when(country === "EMU", lit("EURIBOR"))
                  .otherwise(country),
                concat(lit(" "), regexp_replace(name, "1W", "1D"))
              )
            ),
            (name.like(inflation_uf), name),
            ((name.like(inflation_bonds) || name.like(inflation_linked_bonds)), name)
          ),
          country
        ).as("c2"),

        when(col("timeline").like("%Q%"),
             concat(col("year"), col("timeline")))
          .otherwise(col("year")).as("ejev"),
        col("value")
      )

  restoVariables
}
