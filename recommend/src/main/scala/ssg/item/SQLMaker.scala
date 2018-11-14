package ssg.item

/**
  * Created by moneymall on 26/08/16.
  */
object SQLMaker {

  def getItem : String = {

    """
    SELECT  itemId,
            brandId,
            stdCtgId
    FROM ITEM
    WHERE itemId != ''
    """

  }

}
