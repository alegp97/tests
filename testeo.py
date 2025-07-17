val row1 = mock[Row]
when(row1.getAs[BigDecimal]("scenario_type")).thenReturn(BigDecimal(1))
when(row1.getAs[String]("column_name")).thenReturn("col_abc")
when(row1.getAs[String]("name")).thenReturn("my_Name")

val row2 = mock[Row]
when(row2.getAs[BigDecimal]("scenario_type")).thenReturn(BigDecimal(2))
when(row2.getAs[String]("column_name")).thenReturn("col_def")
when(row2.getAs[String]("name")).thenReturn("my_Name") // mismo nombre para agrupar

val rowGroup = Array(row1, row2)


when(camposOriginals.collect()).thenReturn(rowGroup)



import java.math.BigDecimal as JBigDecimal

when(row1.getAs[JBigDecimal]("scenario_type")).thenReturn(new JBigDecimal("1"))
when(row2.getAs[JBigDecimal]("scenario_type")).thenReturn(new JBigDecimal("2"))
