  // Simulamos la parte de map(...).toList sobre Dataset
  import org.apache.spark.sql.RowFactory
  val nameRow = RowFactory.create("my_Name")
  when(camposOriginals.collect()).thenReturn(Array(nameRow))

  // camposFijos: necesarios si se usan en .map { x => col(...) ... }
  when(camposFijos.select(any[Seq[Column]])).thenReturn(camposFijos)
  when(camposFijos.distinct()).thenReturn(camposFijos)
  val campoFijo1 = RowFactory.create("col_abc")
  val campoFijo2 = RowFactory.create("col_def")
  when(camposFijos.collect()).thenReturn(Array(campoFijo1, campoFijo2))
