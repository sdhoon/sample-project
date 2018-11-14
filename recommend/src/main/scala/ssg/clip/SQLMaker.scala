package ssg.clip

/**
  * Created by 131326 on 2016-11-30.
  */
object SQLMaker {

  def getAssociationBrand() : String = {
  """
    SELECT  DD.brandId,
            DD.kind,
            DD.associationBrandId,
            DD.rank
    FROM (
      SELECT
         CC.brandId,
        'brand' as kind,
        CC.bBrandId as associationBrandId,
        ROW_NUMBER() OVER (PARTITION BY CC.brandId ORDER BY cnt DESC) as rank
      FROM (
        SELECT
                IT.brandId,
                AA.bBrandId,
                MAX(cnt) as cnt
        FROM (
          SELECT B.itemId,
                A.brandId as bBrandId,
                count(A.brandId) as cnt
          FROM ITEM A
          JOIN (
              SELECT  itemId,
                      recomItemId
              FROM itembase
              GROUP BY itemId, recomItemId
          ) B
          ON A.itemId = B.recomItemId
          WHERE A.brandId != ""
          GROUP BY B.itemId, A.brandId
        ) AA join ITEM IT
        ON AA.itemId = IT.itemId
        WHERE IT.brandId != ""
        AND IT.brandId != aa.bBrandId
        AND IT.brandID != "9999999999"
        GROUP BY IT.brandId, AA.bBrandId
        ) CC
      ORDER BY CC.brandId DESC, CC.cnt DESC
    ) DD
    WHERE rank >= 1 AND rank <= 20
  """
  }

  def getBestItem() : String = {
  """
  SELECT
    C.brandId,
    'best' AS kind,
    C.itemId,
    C.rank
  FROM (
    SELECT
     A.brandId,
     A.itemId,
     B.thirdScore,
     ROW_NUMBER() OVER(partition by A.brandId ORDER BY B.thirdScore DESC) rank
    FROM ITEM A
    JOIN SEARCH B
    ON A.itemId = B.itemId
    WHERE A.sellStatCd = 20
    AND A.brandId != ""
    AND A.brandID != "9999999999"
  )  C
  WHERE rank >= 1 AND rank <= 30
  """
  }


  def getNewItem() : String = {
    """
 SELECT
      C.brandId,
      'new' AS kind,
      C.itemId,
      C.rank
  FROM (
   SELECT
         B.brandId,
         B.itemId,
         B.dispStrtDts,
         ROW_NUMBER() OVER(partition by B.brandId ORDER BY B.score DESC, B.dispStrtDts DESC) rank,
         B.score
   FROM (
        SELECT
             A.brandId,
             A.itemId,
             A.dispStrtDts,
             (CASE WHEN S.thirdScore is null THEN
               0
             ELSE
               S.thirdScore
             END
              ) AS score
           FROM (
             SELECT
               I.brandId,
               I.itemId,
               dispStrtDts
             FROM ITEM I
             WHERE I.sellStatCd = 20
             AND I.brandId <> ''
             AND DATEDIFF(CURRENT_DATE,DATE(SUBSTR(I.dispStrtDts,0,10))) >= 0
             AND DATEDIFF(CURRENT_DATE,DATE(SUBSTR(I.dispStrtDts,0,10))) <= 31
           ) A
           JOIN SEARCH S
           ON A.itemId = S.itemId
           AND A.brandId != ""
    ) B
    GROUP BY B.brandId, B.itemId, B.dispStrtDts, B.score
  )C
  WHERE C.rank >= 1 and C.rank <= 30
    """
  }

  def getItemBaseByRank() : String = {
    """
    SELECT
      itemId,
      siteNo,
      recomItemId,
      rank
    FROM (
      SELECT
        itemId,
        siteNo,
        recomItemId,
        ROW_NUMBER() OVER (PARTITION BY itemId,siteNo ORDER BY score DESC) as rank
      FROM itembase
     ) A
     WHERE A.rank >= 1 AND A.rank <= 20
    """
  }
}
