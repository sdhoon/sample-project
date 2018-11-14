package ssg.association

/**
  * Created by moneymall on 23/06/16.
  */
object SQLMaker {

  def getTransaction(): String = {

    """
      SELECT    ordNo
              , concat_ws(',',collect_set(A.itemId)) itemIdCollection
              , count(*)
      FROM DTO A
      JOIN ITEM B
      ON A.itemId = B.itemId
      WHERE B.itemSellTypeCd IN ('10', '20')
      AND B.sellStatCd = '20'
      GROUP BY ordNo
    """

  }

}
