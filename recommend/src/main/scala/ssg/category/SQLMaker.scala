package ssg.category

/**
  * Created by moneymall on 25/07/16.
  */
object SQLMaker {

  def getCategoryBest : String = {
    """
    SELECT
      C.stdCtgId,
      C.stdCtgDclsNm,
      R.itemId,
      R.score
    FROM CATEGORY C
    JOIN
    (
      SELECT
        I.itemId,
        I.stdCtgId,
        S.score
      FROM ITEM I
      JOIN SEARCH S
      ON I.itemId = S.itemId
    ) R
    ON C.stdCtgId = R.stdCtgId
    AND C.useYn = 'Y'
    ORDER BY C.stdCtgDclsId, R.score ASC

    """
  }

}
