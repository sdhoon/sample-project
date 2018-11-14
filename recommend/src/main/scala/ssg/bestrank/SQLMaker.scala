package ssg.bestrank

/**
  * Created by Y.G Chun 15/10/15.
  */
object SQLMaker {

  def getItem() : String = {
    """
  SELECT
      itemId
    , siteNo
    , brandId
    , regDts
    , CASE WHEN nitmAplYn = 'Y' AND regDts >= date_add(current_date, -7) THEN
      'Y'
      ELSE
      'N'
      END AS nitmYn
    , sellPrc
    , prodManufCntryId
    , stdCtgId
  FROM
  (
    SELECT
          A.itemId
        , C.siteNo
        , A.brandId
        , to_date(regDts) regDts
        , MAX(lwstSellPrc) sellPrc
        , A.nitmAplYn
        , A.prodManufCntryId
        , A.stdCtgId
    FROM
    PRE_ITEM A
    JOIN ITEM_PRICE C
    ON A.itemId = C.itemId
    WHERE A.sellStatCd = 20
    GROUP BY A.itemId, C.siteNo, A.brandId, A.regDts, A.nitmAplYn, A.prodManufCntryId, A.stdCtgId
  ) AA
    """
  }

  def getTrack() : String = {
    /**
    http://thehowdy.ssg.com(siteNo=6101)  -> http://howdy.ssg.com(siteNo=6100)
    */
    """
  SELECT
      A.itemId
    ,	A.siteNo
    , MAX(C.sellPrc) sellPrc
  FROM
  (
    SELECT
          itemId
          , ip
          , (
            CASE WHEN siteNo = "6101" THEN
              "6100"
            ELSE
              siteNo
            END
            ) as siteNo
     FROM PRE_TRACK
     WHERE pcid is not null
  ) A
  JOIN ITEM_PRICE C
  ON A.itemId = C.itemId
  GROUP BY A.itemId, A.ip, A.siteNo
    """
  }

  //20170703 유성 : COUNT(*) ordCnt => COUNT(distinct orordNo) ordCnt 로 변경, 실주문 금액 추가
  def getOrdItemCnt() : String = {
    """
  SELECT
      AA.itemId
    , AA.siteNo AS ordSiteNo
    , "" AS trkSiteNo
    , AA.ordCnt
    , "" AS trkItemCnt
    , AA.sellPrc
    , AA.stdCtgId
    , AA.rlordAmt
  FROM
  (
    SELECT
        A.itemId
      , O.siteNo
      , COUNT(distinct orordNo) ordCnt
      , MAX(A.sellPrc) sellPrc
      , MAX(A.stdCtgId) stdCtgId
      , SUM(CAST(ordAmt as float)- CAST(owncoBdnItemDcAmt as float) -CAST(coopcoBdnItemDcAmt as float)) rlordAmt
    FROM PRE_ORD_ITEM O
    JOIN ITEM A
    ON A.itemId = O.itemId
    AND A.siteNo = O.siteNo
    GROUP BY A.itemId, O.siteNo, A.stdCtgId
  ) AA
    """
  }



  def getOrdItemSQL1(ordItemDay:Integer) : String = {
    """
  SELECT
      OO.itemId
    , OO.siteNo
    , OO.ordCnt
    , RANK() OVER(PARTITION BY OO.itemId ORDER BY OO.ordCnt DESC ) ordCntRank
    , RANK() OVER(ORDER BY A.sellPrc DESC) sellPrcRank
  FROM ITEM A
  JOIN
  (
    SELECT
        O.itemId
      , O.siteNo
      , COUNT(itemId) ordCnt
    FROM PRE_ORD_ITEM O
    WHERE O.ordItemDivCd = '011'
    GROUP BY O.itemId, O.siteNo
  ) OO
  ON A.itemId = OO.itemId
  WHERE A.siteNo = OO.siteNo
    """
  }

  def getTrkItemCnt() : String = {
    """
  SELECT
      AA.itemId
    ,	"" AS ordSiteNo
    , AA.siteNo AS trkSiteNo
    , 0 AS ordCnt
    , AA.trkItemCnt
    , AA.sellPrc
    , AA.stdCtgId
    , 0 AS rlordAmt
  FROM
  (
    SELECT
        COUNT(*) trkItemCnt
      , A.itemId
      , B.siteNo
      , MAX(A.sellPrc) sellPrc
      , MAX(A.stdCtgId) stdCtgId
    FROM ITEM A
    JOIN TRACK_ITEM_DTL B
    ON A.itemId = B.itemId
    WHERE A.siteNo = B.siteNo
    GROUP BY A.itemId, B.siteNo, A.stdCtgId
  ) AA
    """
  }

  def getEventCnt() : String = {
    """
  SELECT
        COUNT(*) eventCnt
      , A.itemId
  FROM ITEM A
  JOIN EVENT B
  ON A.itemId = B.itemId
  GROUP BY A.itemId
    """
  }

  def ordTrkUnion() : String = {
    """
  SELECT
        BB.itemId
      ,	BB.siteNo
      , SUM(BB.ordCnt) ordCnt
      , SUM(BB.trkItemCnt) trkItemCnt
      , MAX(BB.sellPrc) sellPrc
      , MAX(BB.stdCtgId) stdCtgId
      , SUM(CAST(BB.rlordAmt as FLOAT)) rlordAmt
    FROM
    (
      SELECT
            AA.itemId
          , AA.ordCnt
          , AA.trkItemCnt
          ,	(
            CASE WHEN AA.ordSiteNo = "" THEN
              AA.trkSiteNo
            WHEN AA.trkSiteNo = "" THEN
              AA.ordSiteNo
            WHEN AA.ordSiteNo != "" THEN
              AA.ordSiteNo
            WHEN AA.trkSiteNo != "" THEN
              AA.trkSiteNo
            END
            ) AS siteNo
          , AA.sellPrc
          , AA.stdCtgId
          , AA.rlordAmt
      FROM
      (
        SELECT * FROM ORD_ITEM_CNT
        UNION ALL
        SELECT * FROM TRACK_ITEM_CNT
      ) AA
    ) BB
    GROUP BY BB.itemId, BB.siteNo
    """
  }


  def itemOrdTrack() : String = {
    """
  SELECT
          BB.itemId
        , BB.siteNo
        , BB.sellPrc
        , BB.brandId
        , BB.nitmYn
        , BB.prodManufCntryId
        , BB.ordCnt
        , BB.trkItemCnt
        , (
            CASE WHEN E.eventCnt > 0 THEN
              "Y"
            ELSE
              "N"
            END
          ) event
        , BB.stdCtgId
        , BB.rlordAmt
  FROM
  (
    SELECT
          AA.itemId
        , MAX(AA.siteNo) siteNo
        , MAX(AA.sellPrc) sellPrc
        , MAX(AA.brandId) brandId
        , MAX(AA.nitmYn) nitmYn
        , MAX(AA.prodManufCntryId) prodManufCntryId
        , SUM(AA.ordCnt) ordCnt
        , SUM(AA.trkItemCnt) trkItemCnt
        , MAX(AA.stdCtgId) stdCtgId
        , CASE
            WHEN SUM(CAST(AA.rlordAmt as FLOAT)) < 0 THEN 0
            ELSE SUM(CAST(AA.rlordAmt as FLOAT))
          END AS rlordAmt
    FROM
    (
      SELECT
            A.itemId
          , A.siteNo
          , A.sellPrc
          , A.brandId
          , A.nitmYn
          , A.prodManufCntryId
          , "" ordCnt
          , "" trkItemCnt
          , A.stdCtgId
          , "" rlordAmt
      FROM ITEM A
      UNION ALL
      SELECT
            B.itemId
          , B.siteNo
          , B.sellPrc
          , "" brandId
          , "" nitmYn
          , "" prodManufCntryId
          , B.ordCnt
          , B.trkItemCnt
          , B.stdCtgId
          , B.rlordAmt rlordAmt
      FROM ORD_TRK_UNION B
    ) AA
    GROUP BY AA.itemId
  ) BB
  LEFT JOIN EVENT_CNT E
  ON BB.itemId = E.itemId
    """
  }



  def ordCntRank() : String = {
    """
  	SELECT
				A.itemId
			,	A.siteNo
			,	A.ordCnt
			, A.trkItemCnt
			, A.event
			,	A.sellPrc
      , A.brandId
      , A.nitmYn
      , A.prodManufCntryId
			, ROW_NUMBER() OVER (ORDER BY CAST(A.ordCnt as FLOAT) DESC) ordCntRank
      , A.stdCtgId
      , A.rlordAmt
		FROM ITEM_ORD_TRK A
    		"""
  }

  //20170703 유 성 : 기존 주문건수*상품가격 으로 계산하던 주문금액을, 실주문금액으로 대체
  def excluding6005() : String = {
    """
  SELECT
      DD.itemId
    ,	DD.siteNo
    , DD.ordCnt
    , DD.ordCntRank
    , DD.trkItemCnt
    , DD.trkItemCntRank
    , DD.sellPrc
    , DD.sellPrcRank
    ,	ROUND(DD.totalScore,4) totalScore
    , DD.totalRank
    , DD.event
    , DD.brandId
    , DD.nitmYn
    , DD.prodManufCntryId
    , DD.totalRank finalRank
    , current_date createDate
    , DD.stdCtgId
  FROM
  (
    SELECT
          CC.itemId
        , CC.siteNo
        , CC.ordCnt
        , CC.ordCntRank
        , CC.trkItemCnt
        , CC.trkItemCntRank
        , CC.sellPrc
        , CC.sellPrcRank
        ,	CC.totalScore
        , ROW_NUMBER() OVER (ORDER BY CAST(CC.totalScore as FLOAT) DESC) totalRank
        ,	CC.event
        , CC.brandId
        , CC.nitmYn
        , CC.prodManufCntryId
        , CC.stdCtgId
    FROM
    (
      SELECT
          BB.itemId
        , BB.siteNo
        , BB.ordCnt
        , BB.ordCntRank
        , BB.trkItemCnt
        , BB.trkItemCntRank
        , BB.sellPrc
        , ROW_NUMBER() OVER (ORDER BY CAST(BB.sellPrc as FLOAT) DESC) sellPrcRank
        , (((1000000/BB.ordCntRank) * 0.5) + ((1000000/BB.sellPrcRank) * 0.2) + ((1000000/BB.trkItemCntRank) * 0.3)) AS totalScore
        , BB.event
        , BB.brandId
        , BB.nitmYn
        , BB.prodManufCntryId
        , BB.stdCtgId
      FROM
      (
        SELECT
            AA.itemId
          , AA.siteNo
          , AA.ordCnt
          , AA.ordCntRank
          , AA.trkItemCnt
          , AA.event
          , AA.trkItemCntRank
          , AA.sellPrc
          , ROW_NUMBER() OVER (ORDER BY CAST(AA.sellPrc as FLOAT) DESC) sellPrcRank
          , AA.brandId
          , AA.nitmYn
          , AA.prodManufCntryId
          , AA.stdCtgId
        FROM
        (
          SELECT
              A.itemId
            ,	A.siteNo
            ,	A.ordCnt
            , RANK() OVER (ORDER BY CAST(A.ordCnt as FLOAT) DESC) ordCntRank
            , A.trkItemCnt
            , A.event
            , ROW_NUMBER() OVER (ORDER BY CAST(A.trkItemCnt as FLOAT) DESC) trkItemCntRank
            , A.rlordAmt sellPrc
            , A.brandId
            , A.nitmYn
            , A.prodManufCntryId
            , A.stdCtgId
          FROM ORD_CNT_RANK A
        ) AA
      )BB
    )CC
  )DD
    """
  }


  def including6005() : String = {
    """
  SELECT
        CC.itemId
      ,	'6005' AS siteNo
      , CC.dispSiteNo
      , CC.ordCnt
      , CC.ordCntRank
      , CC.brandId
      , CC.nitmYn
      , CC.prodManufCntryId
      , CC.trkItemCnt
      , CC.trkItemCntRank
      , CC.sellPrc
      , CC.sellPrcRank
      ,	ROUND(CC.totalScore,4) totalScore
      , CC.totalRank
      , CC.event
      , ROW_NUMBER() OVER (ORDER BY CAST(CC.finalRank as FLOAT) ASC, CAST(totalScore as FLOAT) DESC) finalRank
      , CC.createDate
      , CC.stdCtgId
  FROM
  (
    SELECT
        AA.itemId
      , AA.dispSiteNo
      , AA.ordCnt
      , AA.ordCntRank
      , AA.brandId
      , AA.nitmYn
      , AA.prodManufCntryId
      , AA.trkItemCnt
      , AA.trkItemCntRank
      , AA.sellPrc
      , AA.sellPrcRank
      ,	AA.totalScore
      , AA.totalRank
      , AA.event
      , (
          CASE WHEN AA.event = 'Y' THEN
              AA.totalRank + 100
            ELSE
              AA.totalRank
            END
        ) AS finalRank
      , AA.createDate
      , AA.stdCtgId
    FROM
    (
      SELECT
          B.itemId
        , B.siteNo AS dispSiteNo
        , B.ordCnt
        , B.ordCntRank
        , B.brandId
        , B.nitmYn
        , B.prodManufCntryId
        , B.trkItemCnt
        , B.trkItemCntRank
        , B.sellPrc
        , B.sellPrcRank
        , B.totalScore
        , B.totalRank
        , B.event
        , B.createDate
        , B.stdCtgId
      FROM EXCLUDE_6005 B
    ) AA
  )CC
    """
  }

  def siVillage : String = {
    """
    SELECT
        itemId
      , '6004' as siteNo
      , '6300' as dispSiteNo
      , ordCnt
      , ordCntRank
      , brandId
      , nitmYn
      , prodManufCntryId
      , trkItemCnt
      , trkItemCntRank
      , sellPrc
      , sellPrcRank
      ,	totalScore
      , totalRank
      , event
      , finalRank
      , createDate
      , stdCtgId
     FROM UNION_DATA2
     WHERE dispSiteNo = '6300' and siteNo != '6005'
    """
  }

  def tvHome : String = {
    """
    SELECT
        itemId
      , '6001' as siteNo
      , '6200' as dispSiteNo
      , ordCnt
      , ordCntRank
      , brandId
      , nitmYn
      , prodManufCntryId
      , trkItemCnt
      , trkItemCntRank
      , sellPrc
      , sellPrcRank
      ,	totalScore
      , totalRank
      , event
      , finalRank
      , createDate
      , stdCtgId
     FROM UNION_DATA1
     WHERE dispSiteNo = '6200' and siteNo != '6005'
    """
  }

  def merge : String = {
    """
  SELECT
      itemId
    , dispSiteNo AS siteNo
    , dispSiteNo AS dispSiteNo
    , ordCnt
    , ordCntRank
    , brandId
    , nitmYn
    , prodManufCntryId
    , trkItemCnt
    , trkItemCntRank
    , sellPrc
    , sellPrcRank
    ,	totalScore
    , totalRank
    , event
    , finalRank
    , createDate
    , stdCtgId
  FROM INCLUDE_6005
    """
  }

  def preLoad1 : String = {
    """
  SELECT
      itemId
    , siteNo
    , dispSiteNo
    , ordCnt
    , ordCntRank
    , brandId
    , nitmYn
    , prodManufCntryId
    , trkItemCnt
    , trkItemCntRank
    , sellPrc
    , sellPrcRank
    ,	totalScore
    , totalRank
    , event
    , finalRank
    , createDate
    , stdCtgId
  FROM FINAL_4
    """
  }
/*
  def finalRank : String = {
    """
  SELECT
      itemId
    , siteNo
    , dispSiteNo
    , ordCnt
    , ordCntRank
    , brandId
    , nitmYn
    , prodManufCntryId
    , trkItemCnt
    , trkItemCntRank
    , sellPrc
    , sellPrcRank
    ,	totalScore
    , totalRank
    , event
    , ROW_NUMBER() OVER (PARTITION BY siteNo ORDER BY CAST(finalRank as FLOAT) ASC, CAST(totalScore as FLOAT) DESC) finalRank
    , createDate
    , stdCtgId
    FROM UNION_DATA
      """
  }
*/

  //20170912 유성 신세계상품권 fadeout 용도(임시)
  def finalRank : String = {
    """
     SELECT
            itemId
          , siteNo
          , dispSiteNo
          , ordCnt
          , ordCntRank
          , brandId
          , nitmYn
          , prodManufCntryId
          , trkItemCnt
          , trkItemCntRank
          , sellPrc
          , sellPrcRank
          ,	totalScore
          , totalRank
          , event
          , ROW_NUMBER() OVER (PARTITION BY siteNo ORDER BY CAST(finalRank as FLOAT) ASC, CAST(totalScore as FLOAT) DESC) finalRank
          , createDate
          , stdCtgId
    FROM (
           SELECT
                    itemId
                  , siteNo
                  , dispSiteNo
                  , ordCnt
                  , ordCntRank
                  , brandId
                  , nitmYn
                  , prodManufCntryId
                  , trkItemCnt
                  , trkItemCntRank
                  , sellPrc
                  , sellPrcRank
                  ,	totalScore
                  , totalRank
                  , event
                  , (
                    CASE WHEN itemId = "1000010008801" THEN
                      CAST(finalRank as int) + CAST(datediff(current_date, '2017-09-11') as int) * 5
                    ELSE
                      finalRank
                    END
                    ) as finalRank
                  , createDate
                  , stdCtgId
           FROM (
                SELECT
                    itemId
                  , siteNo
                  , dispSiteNo
                  , ordCnt
                  , ordCntRank
                  , brandId
                  , nitmYn
                  , prodManufCntryId
                  , trkItemCnt
                  , trkItemCntRank
                  , sellPrc
                  , sellPrcRank
                  ,	totalScore
                  , totalRank
                  , event
                  , ROW_NUMBER() OVER (PARTITION BY siteNo ORDER BY CAST(finalRank as FLOAT) ASC, CAST(totalScore as FLOAT) DESC) finalRank
                  , createDate
                  , stdCtgId
                  FROM UNION_DATA
            ) A
       ) B
      """
  }


  def merge3 : String = {
    """
  SELECT
      itemId
    , siteNo
    , dispSiteNo
    , ordCnt
    , ordCntRank
    , brandId
    , nitmYn
    , prodManufCntryId
    , trkItemCnt
    , trkItemCntRank
    , sellPrc
    , sellPrcRank
    ,	totalScore
    , totalRank
    , event
    , finalRank
    , createDate
    , stdCtgId
  FROM MERGE_2
    """
  }

  def merge4 : String = {
    """
  SELECT
      itemId
    , siteNo
    , dispSiteNo
    , ordCnt
    , ordCntRank
    , brandId
    , nitmYn
    , prodManufCntryId
    , trkItemCnt
    , trkItemCntRank
    , sellPrc
    , sellPrcRank
    ,	totalScore
    , totalRank
    , event
    , finalRank
    , createDate
    , stdCtgId
  FROM FINAL_RANK
    """
  }

  def merge5 : String = {
    """
  SELECT
      itemId
    , siteNo
    , dispSiteNo
    , ordCnt
    , ordCntRank
    , brandId
    , nitmYn
    , prodManufCntryId
    , trkItemCnt
    , trkItemCntRank
    , sellPrc
    , sellPrcRank
    ,	totalScore
    , totalRank
    , event
    , finalRank
    , createDate
    , stdCtgId
  FROM MERGE_4
    """
  }



  def prodData : String = {
    """
  SELECT *
  FROM
  (
      SELECT
          date_format(date_add(current_timestamp, 1), 'yMMdd') CRITN_DT
        , SiteNo DISP_SITE_NO
        , dispSiteNo SELL_SITE_NO
        , itemId ITEM_ID
        , brandId BRAND_ID
      , (
          CASE WHEN siteNo = '6002' AND (stdCtgMclsId = '2000000057' OR stdCtgMclsId = '2000000058') THEN
            123456
          ELSE
            0
          END
         ) ORD_CONT
        , 0 ORD_AMT
        , 0 RECOM_REG_CNT
        , (
            CASE WHEN siteNo = '6002' AND (stdCtgMclsId = '2000000057' OR stdCtgMclsId = '2000000058') THEN
              ROUND(10000/finalRank, 4) * 4
            ELSE
              ROUND(10000/finalRank, 4)
            END
           ) eval_scr
        , "" LIVSTK_KIND_CD
        , "" LIVSTK_PAT_CD
        , prodManufCntryId ORPLC_ID
        , nitmYn NITM_YN
        , "BIGDATA" REGPE_ID
        , date_format(current_timestamp, 'y-MM-dd HH:mm:ss') REG_DTS
        , "BIGDATA" MODPE_ID
        , date_format(current_timestamp, 'y-MM-dd HH:mm:ss') MOD_DTS
        , A.stdCtgId
      FROM FINAL_RANK A
      JOIN STD_CTG B
      ON A.stdCtgId = B.stdCtgId
      WHERE A.siteNo <> '6005'
      AND B.useYn = 'Y'

      UNION ALL

      SELECT
          date_format(date_add(current_timestamp, 1), 'yMMdd') CRITN_DT
        , 6005 DISP_SITE_NO
        , dispSiteNo SELL_SITE_NO
        , itemId ITEM_ID
        , brandId BRAND_ID
        , 0 ORD_CONT
        , 0 ORD_AMT
        , 0 RECOM_REG_CNT
        , ROUND(10000/finalRank, 4) eval_scr
        , "" LIVSTK_KIND_CD
        , "" LIVSTK_PAT_CD
        , prodManufCntryId ORPLC_ID
        , nitmYn NITM_YN
        , "BIGDATA" REGPE_ID
        , date_format(current_timestamp, 'y-MM-dd HH:mm:ss') REG_DTS
        , "BIGDATA" MODPE_ID
        , date_format(current_timestamp, 'y-MM-dd HH:mm:ss') MOD_DTS
        , stdCtgId
      FROM FINAL_RANK
      WHERE siteNo = '6005'
  ) AA
  ORDER BY CAST(eval_scr as FLOAT) DESC
  limit 300000
    """
  }

  //20170510 유 성 : LOG SCALE 로 변환
  //20170703 유 성 : sellPrc 를 실주문 금액으로 대체
  def logScale : String = {
  """
        SELECT
             itemId
            ,siteNo
            ,ordCnt
            ,trkitemCnt
            ,event
            ,rlordAmt sellPrc
            ,brandId
            ,nitmYn
            ,prodManufCntryId
            ,stdCtgId
            ,rlordAmt
            ,NVL(LOG10(ordCnt),0) ordCntLog
            ,NVL(LOG10(trkitemCnt),0) trkitemCntLog
            ,NVL(LOG10(rlordAmt),0) rlordAmtLog
        FROM ITEM_ORD_TRK
  """
  }

  //20170510 정규화를 위해 feature별 MIN MAX값 구하기
  def minMax : String = {
  """
    SELECT
         MAX(CAST(ordCntLog AS FLOAT)) maxOrdCnt
        ,MIN(CAST(ordCntLog AS FLOAT)) minOrdCnt
        ,MAX(CAST(trkitemCntLog AS FLOAT)) maxTrkitemCnt
        ,MIN(CAST(trkitemCntLog AS FLOAT)) minTrkitemCnt
        ,MAX(CAST(rlordAmtLog AS FLOAT)) rlordAmtPrc
        ,MIN(CAST(rlordAmtLog AS FLOAT)) rlordAmtPrc
    FROM %s
  """
  }

  //20170510 정규화
  def normalization : String = {
  """
    SELECT
         itemId
        ,siteNo
        ,100 * ((CAST(ordCntLog AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) ordCntNormal
        ,100 * ((CAST(trkitemCntLog AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) trkitemCntNormal
        ,event
        ,100 * ((CAST(rlordAmtLog AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) rlordAmtNormal
        ,brandId
        ,nitmYn
        ,prodManufCntryId
        ,stdCtgId
        ,sellPrc
        ,ordCnt
        ,trkitemCnt
        ,rlordAmt
    FROM LOG_SCALE A
  """
  }

  //20170510 정규화된 값 사용
  def excluding6005_normal() : String = {
    """
  SELECT
      DD.itemId
    ,	DD.siteNo
    , DD.ordCnt
    , DD.ordCntRank
    , DD.trkItemCnt
    , DD.trkItemCntRank
    , DD.sellPrc
    , DD.sellPrcRank
    ,	ROUND(DD.totalScore,4) totalScore
    , DD.totalRank
    , DD.event
    , DD.brandId
    , DD.nitmYn
    , DD.prodManufCntryId
    , DD.totalRank finalRank
    , current_date createDate
    , DD.stdCtgId
  FROM
  (
    SELECT
          CC.itemId
        , CC.siteNo
        , CC.ordCnt
        , CC.ordCntRank
        , CC.trkItemCnt
        , CC.trkItemCntRank
        , CC.sellPrc
        , CC.sellPrcRank
        ,	CC.totalScore
        , ROW_NUMBER() OVER (ORDER BY CAST(CC.totalScore as FLOAT) DESC) totalRank
        ,	CC.event
        , CC.brandId
        , CC.nitmYn
        , CC.prodManufCntryId
        , CC.stdCtgId
    FROM
    (
      SELECT
          BB.itemId
        , BB.siteNo
        , BB.ordCnt
        , BB.ordCntRank
        , BB.trkItemCnt
        , BB.trkItemCntRank
        , BB.sellPrc
        , ROW_NUMBER() OVER (ORDER BY CAST(BB.sellPrc as FLOAT) DESC) sellPrcRank
        , ((CAST(BB.ordCntNormal as float) * 0.5) + (CAST(BB.rlordAmtNormal as float) * 0.2) + (CAST(BB.trkItemCntNormal as float) * 0.3)) AS totalScore
        , BB.event
        , BB.brandId
        , BB.nitmYn
        , BB.prodManufCntryId
        , BB.stdCtgId
      FROM
      (
        SELECT
            AA.itemId
          , AA.siteNo
          , AA.ordCnt
          , AA.ordCntRank
          , AA.trkItemCnt
          , AA.event
          , AA.trkItemCntRank
          , AA.sellPrc
          , ROW_NUMBER() OVER (ORDER BY CAST(AA.sellPrc as FLOAT) DESC) sellPrcRank
          , AA.brandId
          , AA.nitmYn
          , AA.prodManufCntryId
          , AA.stdCtgId
          , AA.ordCntNormal
          , AA.rlordAmtNormal
          , AA.trkItemCntNormal
        FROM
        (
          SELECT
              A.itemId
            ,	A.siteNo
            ,	A.ordCnt
            , RANK() OVER (ORDER BY CAST(A.ordCnt as FLOAT) DESC) ordCntRank
            , A.trkItemCnt
            , A.event
            , ROW_NUMBER() OVER (ORDER BY CAST(A.trkItemCnt as FLOAT) DESC) trkItemCntRank
            , A.rlordAmt sellPrc
            , A.brandId
            , A.nitmYn
            , A.prodManufCntryId
            , A.stdCtgId
            , A.ordCntNormal
            , A.rlordAmtNormal
            , A.trkItemCntNormal
          FROM ORD_CNT_RANK_NORMAL A
        ) AA
      )BB
    )CC
  )DD
    """
  }


  //20170512 3일 주문
  def ordItem3Days : String = {
    """
    SELECT
       *
    FROM TEMP_ORD_ITEM
    WHERE
       ordDt >= '%s'
    """
  }


  // DEBUG
  def getOrdCnt : String = {
    """
       SELECT
          count(*) cnt
       FROM
          PRE_ORD_ITEM
    """


  }


  //20170831 베스트100 성별 연령별
  def getTrackGenBrtdy() : String = {
    /**
    http://thehowdy.ssg.com(siteNo=6101)  -> http://howdy.ssg.com(siteNo=6100)
      */
    """
  SELECT
      A.itemId
    ,	A.siteNo
    , MAX(C.sellPrc) sellPrc
    , A.gen_div_cd
    , A.brtdy
  FROM
  (
    SELECT
          itemId
          , ip
          , (
            CASE WHEN siteNo = "6101" THEN
              "6100"
            ELSE
              siteNo
            END
            ) as siteNo
          , gen_div_cd
          , brtdy
     FROM PRE_TRACK
     WHERE pcid is not null
  ) A
  JOIN ITEM_PRICE C
  ON A.itemId = C.itemId
  AND A.siteNo = C.siteNo
  GROUP BY A.itemId, A.ip, A.siteNo, A.gen_div_cd, A.brtdy
    """
  }

  def MBR_UNIQ() : String = {
    """
        SELECT
                mbr_id,
                max(gen_div_cd) gen_div_cd,
                max(brtdy) brtdy
        FROM MBR3
        WHERE NOT (gen_div_cd = "" and brtdy = "")
        GROUP BY mbr_id
    """
  }

  def preOrdItem() : String = {
    """
        select * from PRE_ORD_ITEM_3DAYS A left join MBR_UNIQ B on A.mbrId = B.mbr_id
    """
  }


  def getOrdItemGenBrtdyCnt() : String = {
    """
  SELECT
      AA.itemId
    , AA.siteNo AS ordSiteNo
    , "" AS trkSiteNo
    , AA.ordCnt
    , "" AS trkItemCnt
    , AA.sellPrc
    , AA.stdCtgId
    , AA.rlordAmt
    , AA.gen_div_cd
    , AA.brtdy
  FROM
  (
    SELECT
        A.itemId
      , O.siteNo
      , COUNT(distinct orordNo) ordCnt
      , MAX(A.sellPrc) sellPrc
      , MAX(A.stdCtgId) stdCtgId
      , SUM(CAST(ordAmt as float)- CAST(owncoBdnItemDcAmt as float) -CAST(coopcoBdnItemDcAmt as float)) rlordAmt
      , gen_div_cd
      , brtdy
    FROM PRE_ORD_ITEM O
    JOIN ITEM A
    ON A.itemId = O.itemId
    AND A.siteNo = O.siteNo
    GROUP BY A.itemId, O.siteNo, A.stdCtgId, O.gen_div_cd, O.brtdy
  ) AA
    """
  }


  def getTrkItemGenBrtdyCnt() : String = {
    """
  SELECT
      AA.itemId
    ,	"" AS ordSiteNo
    , AA.siteNo AS trkSiteNo
    , 0 AS ordCnt
    , AA.trkItemCnt
    , AA.sellPrc
    , AA.stdCtgId
    , 0 AS rlordAmt
    , AA.gen_div_cd
    , AA.brtdy
  FROM
  (
    SELECT
        COUNT(*) trkItemCnt
      , A.itemId
      , B.siteNo
      , MAX(A.sellPrc) sellPrc
      , MAX(A.stdCtgId) stdCtgId
      , B.gen_div_cd
      , B.brtdy
    FROM ITEM A
    JOIN TRACK_ITEM_DTL B
    ON A.itemId = B.itemId
    AND A.siteNo = B.siteNo
    GROUP BY A.itemId, B.siteNo, A.stdCtgId, B.gen_div_cd, B.brtdy
  ) AA
    """
  }

  def ordTrkGenBrtdyUnion() : String = {
    """
 SELECT
        BB.itemId
      ,	BB.siteNo
      , SUM(BB.ordCnt) ordCnt
      , SUM(BB.trkItemCnt) trkItemCnt
      , MAX(BB.sellPrc) sellPrc
      , MAX(BB.stdCtgId) stdCtgId
      , SUM(CAST(BB.rlordAmt as FLOAT)) rlordAmt
      , BB.gen_div_cd
      , BB.brtdy
    FROM
    (
      SELECT
            AA.itemId
          , AA.ordCnt
          , AA.trkItemCnt
          ,	(
            CASE WHEN AA.ordSiteNo = "" THEN
              AA.trkSiteNo
            WHEN AA.trkSiteNo = "" THEN
              AA.ordSiteNo
            WHEN AA.ordSiteNo != "" THEN
              AA.ordSiteNo
            WHEN AA.trkSiteNo != "" THEN
              AA.trkSiteNo
            END
            ) AS siteNo
          , AA.sellPrc
          , AA.stdCtgId
          , AA.rlordAmt
          , AA.gen_div_cd
          , AA.brtdy
      FROM
      (
        SELECT * FROM ORD_ITEM_CNT
        UNION ALL
        SELECT * FROM TRACK_ITEM_CNT
      ) AA
    ) BB
    GROUP BY BB.itemId, BB.siteNo, BB.gen_div_cd, BB.brtdy
    """
  }


  def ITEM_ORD_TRK_RAW() : String = {
    """
  SELECT
          BB.itemId
        , BB.siteNo
        , BB.sellPrc
        , BB.brandId
        , (
            CASE WHEN BB.ordCnt is null THEN
                0
            ELSE
                BB.ordCnt
            END
          ) ordCnt
        , (
            CASE WHEN BB.trkItemCnt is null THEN
                0
            ELSE
                BB.trkItemCnt
            END
        ) trkItemCnt
        , (
            CASE WHEN E.eventCnt > 0 THEN
              "Y"
            ELSE
              "N"
            END
          ) event
        , BB.stdCtgId
        , (
            CASE WHEN BB.rlordAmt is null THEN
                0
            ELSE
                BB.rlordAmt
            END
        ) rlordAmt
        , BB.gen_div_cd
        , BB.brtdy
  FROM
  (
    SELECT
          AA.itemId
        , MAX(AA.siteNo) siteNo
        , MAX(AA.sellPrc) sellPrc
        , MAX(AA.brandId) brandId
        , SUM(AA.ordCnt) ordCnt
        , SUM(AA.trkItemCnt) trkItemCnt
        , MAX(AA.stdCtgId) stdCtgId
        , CASE
            WHEN SUM(CAST(AA.rlordAmt as FLOAT)) < 0 THEN 0
            ELSE SUM(CAST(AA.rlordAmt as FLOAT))
          END AS rlordAmt
        , AA.gen_div_cd
        , AA.brtdy
    FROM
    (
      SELECT
            A.itemId
          , A.siteNo
          , A.sellPrc
          , A.brandId
          , "" ordCnt
          , "" trkItemCnt
          , A.stdCtgId
          , "" rlordAmt
          , "" gen_div_cd
          , "" brtdy
      FROM ITEM A
      UNION ALL
      SELECT
            B.itemId
          , B.siteNo
          , B.sellPrc
          , "" brandId
          , B.ordCnt
          , B.trkItemCnt
          , B.stdCtgId
          , B.rlordAmt
          , B.gen_div_cd
          , B.brtdy
      FROM ORD_TRK_UNION B
    ) AA
    GROUP BY AA.itemId, AA.gen_div_cd, AA.brtdy
  ) BB
  LEFT JOIN EVENT_CNT E
  ON BB.itemId = E.itemId
  WHERE NOT(BB.ordCnt is  null and BB.trkItemCnt is null)
    """
  }


  def ITEM_ORD_TRK_GENBRTDY() : String = {
    """
        select
             itemId
            ,siteNo
            ,sellPrc
            ,brandId
            ,ordCnt
            ,trkItemCnt
            ,event
            ,stdCtgId
            ,rlordAmt
            ,gen_div_cd
            ,brtdy
        from ITEM_ORD_TRK_RAW
        where gen_div_cd != "" and brtdy != ""
    """
  }


  def ITEM_ORD_TRK_GEN() : String = {
    """
        select
            itemId
            ,MAX(siteNo) siteNo
            ,MAX(sellPrc) sellPrc
            ,MAX(brandId) brandId
            ,SUM(ordCnt) ordCnt
            ,SUM(trkItemCnt) trkItemCnt
            ,MAX(event) event
            ,MAX(stdCtgId) stdCtgId
            ,SUM(rlordAmt) rlordAmt
            ,gen_div_cd
            ,"" brtdy
        from ITEM_ORD_TRK_RAW
        where gen_div_cd != ""
        group by itemId, gen_div_cd
    """
  }

  def ITEM_ORD_TRK_BRTDY() : String = {
    """
        select
            itemId
            ,MAX(siteNo) siteNo
            ,MAX(sellPrc) sellPrc
            ,MAX(brandId) brandId
            ,SUM(ordCnt) ordCnt
            ,SUM(trkItemCnt) trkItemCnt
            ,MAX(event) event
            ,MAX(stdCtgId) stdCtgId
            ,SUM(rlordAmt) rlordAmt
            ,"" gen_div_cd
            ,brtdy
        from ITEM_ORD_TRK_RAW
        where brtdy != ""
        group by itemId, brtdy
    """
  }


  def ITEM_ORD_TRK() : String = {
    """
        select
            itemId
            ,MAX(siteNo) siteNo
            ,MAX(sellPrc) sellPrc
            ,MAX(brandId) brandId
            ,SUM(ordCnt) ordCnt
            ,SUM(trkItemCnt) trkItemCnt
            ,MAX(event) event
            ,MAX(stdCtgId) stdCtgId
            ,SUM(rlordAmt) rlordAmt
            ,"" gen_div_cd
            ,"" brtdy
        from ITEM_ORD_TRK_RAW
        group by itemId
    """
  }


  def LOG_SCALE_DYNAMIC : String = {
    """
          SELECT
             itemId
            ,siteNo
            ,ordCnt
            ,trkItemCnt
            ,event
            ,sellPrc
            ,brandId
            ,stdCtgId
            ,rlordAmt
            ,NVL(LOG10(ordCnt),0) ordCntLog
            ,NVL(LOG10(trkItemCnt),0) trkItemCntLog
            ,NVL(LOG10(rlordAmt),0) rlordAmtLog
            ,gen_div_cd
            ,brtdy
        FROM %s
    """
  }


  def normalization_dynamic : String = {
    """
    SELECT
         itemId
        ,siteNo
        ,100 * ((CAST(ordCntLog AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) ordCntNormal
        ,100 * ((CAST(trkItemCntLog AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) trkItemCntNormal
        ,event
        ,100 * ((CAST(rlordAmtLog AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) rlordAmtNormal
        ,brandId
        ,stdCtgId
        ,sellPrc
        ,ordCnt
        ,trkItemCnt
        ,rlordAmt
        ,gen_div_cd
        ,brtdy
      FROM %s A
    """
  }


  def TOTAL_SCORE() : String = {
    """
            SELECT
                  AA.itemId
                , AA.siteNo
                , AA.siteNo dispSiteNo
                , AA.ordCnt
                , AA.trkItemCnt
                , AA.rlordAmt
                , ROUND(((CAST(AA.ordCntNormal as float) * 0.5) + (CAST(AA.rlordAmtNormal as float) * 0.2) + (CAST(AA.trkItemCntNormal as float) * 0.3)),4) totalScore
                , AA.sellPrc
                , AA.event
                , AA.brandId
                , current_date createDate
                , AA.stdCtgId
                , AA.gen_div_cd
                , AA.brtdy
            FROM
                %s AA
    """
  }


  def COPY_SITE_AS_6004() : String = {
    """
        SELECT
            AA.itemId
          , '6004' AS siteNo
          , AA.siteNo dispSiteNo
          , AA.ordCnt
          , AA.trkItemCnt
          , AA.rlordAmt
          ,	AA.totalScore
          , AA.sellPrc
          , AA.event
          , AA.brandId
          , AA.createDate
          , AA.stdCtgId
          , AA.gen_div_cd
          , AA.brtdy
        FROM %s AA
        WHERE siteNo = '%s'
    """
  }


  def FINAL_RANK : String = {
    """
    SELECT
          itemId
        , siteNo
        , dispSiteNo
        , ordCnt
        , trkItemCnt
        , rlordAmt
        , totalScore
        , sellPrc
        , event
        , brandId
        , createDate
        , stdCtgId
        , gen_div_cd
        , brtdy
        , ROUND(10000/finalRank, 4) finalRank
    FROM (
      SELECT
            itemId
          , siteNo
          , dispSiteNo
          , ordCnt
          , trkItemCnt
          , rlordAmt
          , totalScore
          , sellPrc
          , event
          , brandId
          , createDate
          , stdCtgId
          , gen_div_cd
          , brtdy
          , ROW_NUMBER() OVER (%s ORDER BY CAST(finalRank as FLOAT) ASC) finalRank
      FROM (
              SELECT
                    itemId
                  , siteNo
                  , dispSiteNo
                  , ordCnt
                  , trkItemCnt
                  , rlordAmt
                  , totalScore
                  , sellPrc
                  , event
                  , brandId
                  , createDate
                  , stdCtgId
                  , gen_div_cd
                  , brtdy
                  , (
                    CASE WHEN event = 'Y' THEN
                        CAST(finalRank as int) + 110
                      ELSE
                        finalRank
                      END
                  ) AS finalRank
              FROM (
                  SELECT
                        itemId
                      , siteNo
                      , dispSiteNo
                      , ordCnt
                      , trkItemCnt
                      , rlordAmt
                      , totalScore
                      , sellPrc
                      , event
                      , brandId
                      , createDate
                      , stdCtgId
                      , gen_div_cd
                      , brtdy
                      , ROW_NUMBER() OVER (%s ORDER BY CAST(totalScore as FLOAT) DESC) finalRank
                 FROM %s
             ) A
             WHERE finalRank <= 200
      ) B
      ) C
    """
  }


  def getDistcintItemId : String = {
    """
            select itemId, siteNo, dispSiteNo, max(brandId) brandId from %s group by itemId, siteNo, dispSiteNo
    """
  }


  def getRankByBrtdy : String = {
    """
        select
                  itemId
                , siteNo
                , dispSiteNo
                , brandId
                , finalRank %s
        from FINAL_RANK_BRTDY where brtdy = %s
    """
  }

  def getRankByGen : String = {
    """
        select
                  itemId
                , siteNo
                , dispSiteNo
                , brandId
                , finalRank %s
        from FINAL_RANK_GEN where gen_div_cd = %s
    """
  }

  def getRankByGenBrtdy : String = {
    """
        select
                  itemId
                , siteNo
                , dispSiteNo
                , brandId
                , finalRank %s
        from FINAL_RANK_GENBRTDY where gen_div_cd = %s and brtdy = %s
    """
  }

  def getRankByAllAll : String = {
    """
        select
                  itemId
                , siteNo
                , dispSiteNo
                , brandId
                , finalRank %s
        from FINAL_RANK
    """
  }

  def FINAL_RANK_STEP_01: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,NVL(TWET_ALL_GEN_EVAL_SCR, "0")    TWET_ALL_GEN_EVAL_SCR
        FROM distinctItem A
        FULL OUTER JOIN FINAL_RANK_BRTDY_20        B   ON A.itemId = B.itemId AND A.siteNo = B.siteNo AND A.dispSiteNo = B.dispSiteNo
    """
  }

  def FINAL_RANK_STEP_02: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,NVL(THIT_ALL_GEN_EVAL_SCR, "0")    THIT_ALL_GEN_EVAL_SCR
        FROM FINAL_RANK_STEP_01 A
        FULL OUTER JOIN FINAL_RANK_BRTDY_30        C   ON A.itemId = C.itemId AND A.siteNo = C.siteNo AND A.dispSiteNo = C.dispSiteNo
    """
  }

  def FINAL_RANK_STEP_03: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,NVL(FORT_ALL_GEN_EVAL_SCR, "0")    FORT_ALL_GEN_EVAL_SCR
        FROM FINAL_RANK_STEP_02 A
        FULL OUTER JOIN FINAL_RANK_BRTDY_40        D   ON A.itemId = D.itemId AND A.siteNo = D.siteNo AND A.dispSiteNo = D.dispSiteNo
    """
  }

  def FINAL_RANK_STEP_04: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,NVL(FIFT_ALL_GEN_EVAL_SCR, "0")    FIFT_ALL_GEN_EVAL_SCR
        FROM FINAL_RANK_STEP_03 A
        FULL OUTER JOIN FINAL_RANK_BRTDY_50        E   ON A.itemId = E.itemId AND A.siteNo = E.siteNo AND A.dispSiteNo = E.dispSiteNo
    """
  }


  def FINAL_RANK_STEP_05: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,NVL(TWET_MALE_EVAL_SCR, "0")    TWET_MALE_EVAL_SCR
        FROM FINAL_RANK_STEP_04 A
        FULL OUTER JOIN FINAL_RANK_GEN_10_BRTDY_20 F   ON A.itemId = F.itemId AND A.siteNo = F.siteNo AND A.dispSiteNo = F.dispSiteNo
    """
  }

  def FINAL_RANK_STEP_06: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,TWET_MALE_EVAL_SCR
            ,NVL(THIT_MALE_EVAL_SCR, "0")    THIT_MALE_EVAL_SCR
        FROM FINAL_RANK_STEP_05 A
        FULL OUTER JOIN FINAL_RANK_GEN_10_BRTDY_30 G   ON A.itemId = G.itemId AND A.siteNo = G.siteNo AND A.dispSiteNo = G.dispSiteNo
    """
  }


  def FINAL_RANK_STEP_07: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,TWET_MALE_EVAL_SCR
            ,THIT_MALE_EVAL_SCR
            ,NVL(FORT_MALE_EVAL_SCR, "0")    FORT_MALE_EVAL_SCR
        FROM FINAL_RANK_STEP_06 A
        FULL OUTER JOIN FINAL_RANK_GEN_10_BRTDY_40 H   ON A.itemId = H.itemId AND A.siteNo = H.siteNo AND A.dispSiteNo = H.dispSiteNo
    """
  }



  def FINAL_RANK_STEP_08: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,TWET_MALE_EVAL_SCR
            ,THIT_MALE_EVAL_SCR
            ,FORT_MALE_EVAL_SCR
            ,NVL(FIFT_MALE_EVAL_SCR, "0")    FIFT_MALE_EVAL_SCR
        FROM FINAL_RANK_STEP_07 A
        FULL OUTER JOIN FINAL_RANK_GEN_10_BRTDY_50 I   ON A.itemId = I.itemId AND A.siteNo = I.siteNo AND A.dispSiteNo = I.dispSiteNo
    """
  }



  def FINAL_RANK_STEP_09: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,TWET_MALE_EVAL_SCR
            ,THIT_MALE_EVAL_SCR
            ,FORT_MALE_EVAL_SCR
            ,FIFT_MALE_EVAL_SCR
            ,NVL(TWET_FMALE_EVAL_SCR, "0")    TWET_FMALE_EVAL_SCR
        FROM FINAL_RANK_STEP_08 A
        FULL OUTER JOIN FINAL_RANK_GEN_20_BRTDY_20 J   ON A.itemId = J.itemId AND A.siteNo = J.siteNo AND A.dispSiteNo = J.dispSiteNo
    """
  }


  def FINAL_RANK_STEP_10: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,TWET_MALE_EVAL_SCR
            ,THIT_MALE_EVAL_SCR
            ,FORT_MALE_EVAL_SCR
            ,FIFT_MALE_EVAL_SCR
            ,TWET_FMALE_EVAL_SCR
            ,NVL(THIT_FMALE_EVAL_SCR, "0")    THIT_FMALE_EVAL_SCR
        FROM FINAL_RANK_STEP_09 A
        FULL OUTER JOIN FINAL_RANK_GEN_20_BRTDY_30 K   ON A.itemId = K.itemId AND A.siteNo = K.siteNo AND A.dispSiteNo = K.dispSiteNo
    """
  }


  def FINAL_RANK_STEP_11: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,TWET_MALE_EVAL_SCR
            ,THIT_MALE_EVAL_SCR
            ,FORT_MALE_EVAL_SCR
            ,FIFT_MALE_EVAL_SCR
            ,TWET_FMALE_EVAL_SCR
            ,THIT_FMALE_EVAL_SCR
            ,NVL(FORT_FMALE_EVAL_SCR, "0")    FORT_FMALE_EVAL_SCR
        FROM FINAL_RANK_STEP_10 A
        FULL OUTER JOIN FINAL_RANK_GEN_20_BRTDY_40 L   ON A.itemId = L.itemId AND A.siteNo = L.siteNo AND A.dispSiteNo = L.dispSiteNo
    """
  }


  def FINAL_RANK_STEP_12: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,TWET_MALE_EVAL_SCR
            ,THIT_MALE_EVAL_SCR
            ,FORT_MALE_EVAL_SCR
            ,FIFT_MALE_EVAL_SCR
            ,TWET_FMALE_EVAL_SCR
            ,THIT_FMALE_EVAL_SCR
            ,FORT_FMALE_EVAL_SCR
            ,NVL(FIFT_FMALE_EVAL_SCR, "0")    FIFT_FMALE_EVAL_SCR
        FROM FINAL_RANK_STEP_11 A
        FULL OUTER JOIN FINAL_RANK_GEN_20_BRTDY_50 M   ON A.itemId = M.itemId AND A.siteNo = M.siteNo AND A.dispSiteNo = M.dispSiteNo
    """
  }

  def FINAL_RANK_STEP_13: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,TWET_MALE_EVAL_SCR
            ,THIT_MALE_EVAL_SCR
            ,FORT_MALE_EVAL_SCR
            ,FIFT_MALE_EVAL_SCR
            ,TWET_FMALE_EVAL_SCR
            ,THIT_FMALE_EVAL_SCR
            ,FORT_FMALE_EVAL_SCR
            ,FIFT_FMALE_EVAL_SCR
            ,NVL(ALL_AGEGRP_MALE_EVAL_SCR, "0")    ALL_AGEGRP_MALE_EVAL_SCR
        FROM FINAL_RANK_STEP_12 A
        FULL OUTER JOIN FINAL_RANK_GEN_10          N   ON A.itemId = N.itemId AND A.siteNo = N.siteNo AND A.dispSiteNo = N.dispSiteNo
    """
  }

  def FINAL_RANK_STEP_14: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,TWET_MALE_EVAL_SCR
            ,THIT_MALE_EVAL_SCR
            ,FORT_MALE_EVAL_SCR
            ,FIFT_MALE_EVAL_SCR
            ,TWET_FMALE_EVAL_SCR
            ,THIT_FMALE_EVAL_SCR
            ,FORT_FMALE_EVAL_SCR
            ,FIFT_FMALE_EVAL_SCR
            ,ALL_AGEGRP_MALE_EVAL_SCR
            ,NVL(ALL_AGEGRP_FMALE_EVAL_SCR, "0")    ALL_AGEGRP_FMALE_EVAL_SCR
        FROM FINAL_RANK_STEP_13 A
        FULL OUTER JOIN FINAL_RANK_GEN_20          O   ON A.itemId = O.itemId AND A.siteNo = O.siteNo AND A.dispSiteNo = O.dispSiteNo
    """
  }

  def FINAL_RANK_STEP_15: String = {
    """
        SELECT
            A.itemId
            ,A.siteNo
            ,A.dispSiteNo
            ,A.brandId
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,TWET_MALE_EVAL_SCR
            ,THIT_MALE_EVAL_SCR
            ,FORT_MALE_EVAL_SCR
            ,FIFT_MALE_EVAL_SCR
            ,TWET_FMALE_EVAL_SCR
            ,THIT_FMALE_EVAL_SCR
            ,FORT_FMALE_EVAL_SCR
            ,FIFT_FMALE_EVAL_SCR
            ,ALL_AGEGRP_MALE_EVAL_SCR
            ,ALL_AGEGRP_FMALE_EVAL_SCR
            ,NVL(ALL_AGEGRP_GEN_EVAL_SCR, "0") ALL_AGEGRP_GEN_EVAL_SCR
        FROM FINAL_RANK_STEP_14 A
        FULL OUTER JOIN FINAL_RANK_ALL_ALL         P   ON A.itemId = P.itemId AND A.siteNo = P.siteNo AND A.dispSiteNo = P.dispSiteNo
    """
  }

  def BDT_BEST100: String = {
    """
        SELECT
             date_format(date_add(current_timestamp, 1), 'yMMdd') CRITN_DT
            ,A.siteNo DISP_SITE_NO
            ,A.itemId ITEM_ID
            ,TWET_MALE_EVAL_SCR
            ,TWET_FMALE_EVAL_SCR
            ,TWET_ALL_GEN_EVAL_SCR
            ,THIT_MALE_EVAL_SCR
            ,THIT_FMALE_EVAL_SCR
            ,THIT_ALL_GEN_EVAL_SCR
            ,FORT_MALE_EVAL_SCR
            ,FORT_FMALE_EVAL_SCR
            ,FORT_ALL_GEN_EVAL_SCR
            ,FIFT_MALE_EVAL_SCR
            ,FIFT_FMALE_EVAL_SCR
            ,FIFT_ALL_GEN_EVAL_SCR
            ,ALL_AGEGRP_MALE_EVAL_SCR
            ,ALL_AGEGRP_FMALE_EVAL_SCR
            ,ALL_AGEGRP_GEN_EVAL_SCR
            ,"BIGDATA" REGPE_ID
            ,date_format(current_timestamp, 'y-MM-dd HH:mm:ss') REG_DTS
            ,"BIGDATA" MODPE_ID
            ,date_format(current_timestamp, 'y-MM-dd HH:mm:ss') MOD_DTS
        FROM FINAL_RANK_STEP_15 A
    """
  }

}