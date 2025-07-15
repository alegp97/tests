val sqlCtx  = mock[SQLContext](RETURNS_DEEP_STUBS)
    val engine  = mock[DataFrame](RETURNS_DEEP_STUBS)

    // tabla "sae_engine" → engine
    when(sqlCtx.table("sourcedb.sae_engine")).thenReturn(engine)

    // Stub global hasta collect()
    when(
      engine.where(any[Column]())
            .map(any[Function1[Row,_]]())(any[Encoder[_]]())
            .collect()
    ).thenReturn(Array("dummy_id1","dummy_id2"))

    val df = BoardDataUtil.calculateDFModels("sourcedb", K.CREDIT, sqlCtx)






 val sqlCtx  = mock[SQLContext]          // sin deep-stubs
    val engine  = mock[DataFrame]           // sin deep-stubs
    val idLogit = mock[DataFrame]           // after where
    val dsStr   = mock[DataFrame]           // after map (Dataset[String])

    when(sqlCtx.table("sourcedb.sae_engine")).thenReturn(engine)

    // 1. where → idLogit
    when(engine.where(any[Column]())).thenReturn(idLogit)

    // 2. map → dsStr
    when(idLogit.map(any[Function1[Row,_]]())(any[Encoder[_]]()))
      .thenReturn(dsStr)

    // 3. collect sobre dsStr
    when(dsStr.collect()).thenReturn(Array("dummy_id1","dummy_id2"))

    val df = BoardDataUtil.calculateDFModels("sourcedb", K.CREDIT, sqlCtx)









 private val smartCollect: Answers = (inv: InvocationOnMock) => {
    if (inv.getMethod.getName == "collect")
      Array("dummy_id1","dummy_id2")
    else
      Answers.RETURNS_DEEP_STUBS.answer(inv)
  }



    val sqlCtx = mock[SQLContext](RETURNS_DEEP_STUBS)
    val engine = mock[DataFrame](
      withSettings().defaultAnswer(smartCollect)
    )

    when(sqlCtx.table("sourcedb.sae_engine")).thenReturn(engine)

    val df = BoardDataUtil.calculateDFModels("sourcedb", K.CREDIT, sqlCtx)





