val colA = mock[Column]
when(colA.toString).thenReturn("a")

val colB = mock[Column]
when(colB.toString).thenReturn("b")

val big = List(colA, colB)
val small = List(colB)

val result = BoardDataUtil.columnNotInColumn(big, small)
assert(result.map(_.toString) == List("a"))
