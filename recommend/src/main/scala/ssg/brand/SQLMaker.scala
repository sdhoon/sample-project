package ssg.brand


/**
  * Created by moneymall on 13/07/16.
  */
object SQLMaker {

  def getBrandRank : String = {
    """
    SELECT
      B.brandId,
      B.brandNm,
      R.itemId,
      R.score
    FROM BRAND B
    JOIN
    (
      SELECT
        I.itemId,
        I.brandId,
        S.score
      FROM ITEM I
      JOIN SEARCH S
      ON I.itemId = S.itemId
    ) R
    ON B.brandId = R.brandId
    AND B.useYn = 'Y'
    ORDER BY B.brandId, R.score ASC
    """
  }

}
