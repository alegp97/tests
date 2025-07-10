test("calculateRepartition should cover all repartition branches") {
  assert(BoardDataUtil.calculateRepartition(500000) == 1)
  assert(BoardDataUtil.calculateRepartition(1000000) == 2)
  assert(BoardDataUtil.calculateRepartition(2000000) == 3)
  assert(BoardDataUtil.calculateRepartition(3000000) == 4)
  assert(BoardDataUtil.calculateRepartition(4000000) == 5)
  assert(BoardDataUtil.calculateRepartition(5000000) == 6)
  assert(BoardDataUtil.calculateRepartition(6000000) == 7)
  assert(BoardDataUtil.calculateRepartition(7000000) == 8)
  assert(BoardDataUtil.calculateRepartition(8000000) == 9)
  assert(BoardDataUtil.calculateRepartition(9000000) == 10)
  assert(BoardDataUtil.calculateRepartition(10000000) == 20)
  assert(BoardDataUtil.calculateRepartition(20000000) == 40)
  assert(BoardDataUtil.calculateRepartition(40000000) == 60)
  assert(BoardDataUtil.calculateRepartition(60000000) == 80)
  assert(BoardDataUtil.calculateRepartition(80000000) == 100)
  assert(BoardDataUtil.calculateRepartition(150000000) == 100) // fuera del rango, cae en el último else
}
