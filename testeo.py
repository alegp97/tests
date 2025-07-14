when(idLogitDF.select(any[Column])).thenReturn(idLogitDF)
when(idLogitDF.distinct()).thenReturn(idLogitDF)

// 3️⃣  Stub directo hasta collect()
import org.apache.spark.sql.{Encoder, Encoders, Row}
when(
  idLogitDF
    .map(any[Function1[Row,String]]())          // 1ª lista de parámetros
    (any[Encoder[String]]())                    // 2ª (implícitos)
    .collect()
).thenReturn(Array("dummy_id1","dummy_id2"))
