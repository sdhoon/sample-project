package ssg.collaboration

/**
  * Created by moneymall on 17/03/16.
  */
object SQLMaker {


  def getItem() : String = {
    """
          SELECT
                  I.itemId
                , I.recommendedItemId
                , S.stdCtgSclsId
                , S.stdCtgId
                , S.stdCtgLcLsNm
                , S.stdCtgMclsNm
                , S.stdCtgSclsNm
                , S.stdCtgDclsNm
          FROM   ITEM_BASED I
               JOIN ITEM A
               JOIN STD_CTG S
           ON I.itemId = A.itemId
           AND A.stdCtgId = S.stdCtgId
           AND A.srchPsblYn = 'Y'

    """
  }


  def getItem2() : String = {
    """
    SELECT
            I.itemId
          , I.recommendedItemId
          , S.stdCtgSclsId stdCtgSclsId
          , S.priorStdCtgId
          , I.score
          , S.stdCtgId
          , S.stdCtgLcLsNm recomStdCtgLcLsNm
          , S.stdCtgMclsNm recomStdCtgMclsNm
          , S.stdCtgSclsNm recomStdCtgSclsNm
          , S.stdCtgDclsNm recomStdCtgDclsNm
    FROM   ITEM_BASED I
         JOIN ITEM A
         JOIN STD_CTG S
     ON I.recommendedItemId = A.itemId
     AND A.stdCtgId = S.stdCtgId
     AND A.srchPsblYn = 'Y'
    """
  }

  def getCategory() : String = {
    """
      SELECT
            A.itemId
            , B.recommendedItemId
            , B.score
            , A.stdCtgLcLsNm
            , A.stdCtgMclsNm
            , A.stdCtgSclsNm
            , A.stdCtgDclsNm
            , B.recomStdCtgLcLsNm
            , B.recomStdCtgMclsNm
            , B.recomStdCtgSclsNm
            , B.recomStdCtgDclsNm
      FROM  TARGET_ITEM A
            JOIN RECOM_ITEM B
      ON A.itemId = B.itemID
      AND A.recommendedItemId = B.recommendedItemId
      AND A.stdCtgSclsId = B.stdCtgSclsId
      ORDER BY A.itemId , B.score DESC
    """
  }

  def getSiteNo() : String = {
    """
    SELECT
        A.itemId
      , A.recommendedItemId
      , B.siteNo
      , A.score
      , A.stdCtgLcLsNm
      , A.stdCtgMclsNm
      , A.stdCtgSclsNm
      , A.stdCtgDclsNm
      , A.recomStdCtgLcLsNm
      , A.recomStdCtgMclsNm
      , A.recomStdCtgSclsNm
      , A.recomStdCtgDclsNm
    FROM CATEGORIZED_ITEM A
    JOIN SITE B
    ON A.recommendedItemId = B.itemId
    """
  }

  def getCategoryOrder() : String = {
    """
      SELECT
            A.itemId
            , B.recommendedItemId
            , B.score
            , A.stdCtgLcLsNm
            , A.stdCtgMclsNm
            , A.stdCtgSclsNm
            , A.stdCtgDclsNm
            , B.recomStdCtgLcLsNm
            , B.recomStdCtgMclsNm
            , B.recomStdCtgSclsNm
            , B.recomStdCtgDclsNm
      FROM  TARGET_ITEM A
            JOIN RECOM_ITEM B
      ON A.itemId = B.itemID
      AND A.recommendedItemId = B.recommendedItemId
      ORDER BY A.itemId , B.score DESC
    """
  }

  def getClickCountById() : String = {
    """
    SELECT
            fsid
          ,itemId
          ,count(*)
          ,timestamp
    FROM CLICK
    GROUP BY fsid, itemId, timestamp
    """
  }

  def getOrdCountById() : String = {
    """
    SELECT
            ordNo
          ,itemId
          ,count(*)
          ,timestamp
    FROM ORDER
    GROUP BY ordNo, itemId, timestamp
    """
  }



  def getAvgScore() : String = {
    """
    SELECT
               CASE WHEN ISNOTNULL(A.itemId) AND ISNULL(B.itemId) THEN A.itemId
                    WHEN ISNOTNULL(B.itemId) AND ISNULL(A.itemId) THEN B.itemId
                    ELSE A.itemId
                END AS itemId,
                CASE WHEN ISNOTNULL(A.stdCtg) AND ISNULL(B.stdCtg) THEN A.stdCtg
                     WHEN ISNOTNULL(B.stdCtg) AND ISNULL(A.stdCtg) THEN B.stdCtg
                     ELSE A.stdCtg
                END AS stdCtg,
                CASE WHEN ISNOTNULL(A.recommendItemId) AND ISNULL(B.recommendItemId) THEN A.recommendItemId
                    WHEN ISNOTNULL(B.recommendItemId) AND ISNULL(A.recommendItemId) THEN B.recommendItemId
                    ELSE A.recommendItemId
                END AS recommendItemId,
                CASE WHEN ISNOTNULL(A.recommendStdCtg) AND ISNULL(B.recommendStdCtg) THEN A.recommendStdCtg
                    WHEN ISNOTNULL(B.recommendStdCtg) AND ISNULL(A.recommendStdCtg) THEN B.recommendStdCtg
                    ELSE A.recommendStdCtg
                END AS recommendStdCtg,
                CASE WHEN ISNOTNULL(A.method) AND ISNULL(B.method) THEN A.method
                    WHEN ISNOTNULL(B.method) AND ISNULL(A.method) THEN B.method
                    ELSE A.method
                END AS method
             , A.score
             , B.score
             , CASE WHEN isnotnull(A.score) AND isnotnull(B.score) THEN (A.score + B.score) / 2
                    WHEN isnull(A.score) AND isnotnull(B.score) THEN B.score
                    WHEN isnull(B.score) AND isnotnull(A.score) THEN A.score
               END AS score,
               CASE WHEN ISNOTNULL(A.siteNo) AND ISNULL(B.siteNo) THEN A.siteNo
                    WHEN ISNOTNULL(B.siteNo) AND ISNULL(A.siteNo) THEN B.siteNo
                    ELSE A.siteNo
               END AS siteNo
    FROM ITEMBASE_YESTERDAY A
    FULL OUTER JOIN ITEMBASE_TODAY B
    ON A.itemId = B.ItemId
    AND A.recommendItemId = B.recommendItemId
    AND A.siteNo = B.siteNo
    """
  }


}