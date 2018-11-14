package ssg.searchbest

/**
  * Created by moneymall on 16/01/15.
  */

/**
  * Created by reated by Sung Ryu on 16/01/15.
  */
object SQLMaker {


  def updateLoginHistory() : String = {
    """
      SELECT A.*
      FROM  (
          SELECT
                  MAX(mbrId) mbrId
                 ,pcid
                 ,MAX(updateDate) updateDate
          FROM
                MERGE_LOGIN_HISTORY
          GROUP BY
                pcid
      ) A
      WHERE
          A.updateDate >= add_months(date_sub(current_date,1),-6)
    """
  }


  def todayLoginInfo() : String = {
    """
      SELECT
             mbrId
            ,pcid
            ,date_sub(current_date,1) updateDate
      FROM
            TRACKING_LOG
      WHERE
                visitDts >= date_sub(current_date, 3)
            AND visitDts < date_sub(current_date, 1)
            AND mbrId IS NOT NULL
            AND mbrId <> ''
            AND pcid IS NOT NULL
            AND pcid <> ''
      GROUP BY
             mbrId
            ,pcid
    """
  }


  def distinctItemList() : String = {
    """
      SELECT
             itemId
            ,max(siteNo) siteNo
      FROM
            TODAY_ITEM_PRICE
      GROUP BY
            itemId
    """
  }


  def dwSrchItemBest() : String = {
    """
      SELECT
                CONCAT(substring(current_date,6,2),substring(current_date, 9, 2)) createDate
               ,A.itemId
               ,B.siteNo          dispSiteNo
               ,B.siteNo          sellSiteNo
               ,A.purchaseCount
               ,A.purchaseAmt
               ,A.evarScr
               ,A.convrt
               ,A.clickMbrId
               ,A.clickCount
               ,A.regpeId
               ,A.regDts
               ,A.modpeId
               ,A.modDts
      FROM  (
              SELECT
                       convrt
                      ,clickCount
                      ,clickMbrId
                      ,CONCAT(current_date, ' ',hour(current_timestamp), ':', minute(current_timestamp), ':', second(current_timestamp))    modDts
                      ,'infra'              modpeId
                      ,CONCAT(current_date, ' ',hour(current_timestamp), ':', minute(current_timestamp), ':', second(current_timestamp))    regDts
                      ,'infra'              regpeId
                      ,ROUND(0.3*(1000000/orderCountRank)+0.5*(1000000/orderAmtRank)+0.7*(1000000/clickRank),4) evarScr
                      ,purchaseAmt
                      ,purchaseCount
                      ,itemId
              FROM  (
                   SELECT
                           MIN(ROUND(purchaseCount/clickCount*purchaseMbrId/clickMbrId*100,2)) convrt
                          ,MIN(clickCount)      clickCount
                          ,MIN(clickMbrId)      clickMbrId
                          ,MIN(orderCountRank)  orderCountRank
                          ,MIN(orderAmtRank)    orderAmtRank
                          ,MIN(clickRank)       clickRank
                          ,MAX(purchaseAmt)     purchaseAmt
                          ,MAX(purchaseCount)   purchaseCount
                          ,itemId
                   FROM
                          BEST_RANK_LIST
                   GROUP BY
                          itemId
              ) A ORDER BY evarScr DESC
      ) A INNER JOIN
            DISTINCT_ITEM_LIST B
          ON
            A.itemId = B.itemId
          ORDER BY
              A.evarScr DESC
    """
  }

  def bestRankList() : String = {
    """
		SELECT
           A.*
          ,B.orderCountRank
          ,C.orderAmtRank
          ,D.clickRank
    FROM
          EACH_ITEM_LOG A
    INNER JOIN
          BEST_ORDER_COUNT_LIST B
    ON
          A.itemId = B.itemId
    INNER JOIN
          BEST_ORDER_AMT_LIST C
    ON
          A.itemId = C.itemId
    INNER JOIN
          BEST_CLICK_COUNT_LIST D
    ON
          A.itemId = D.itemId

    """
  }



  def bestOrderCountList() : String = {
    """
		SELECT
           itemId
          ,ROW_NUMBER() OVER (ORDER BY purchaseCount*openWeight DESC) orderCountRank
    FROM
          EACH_ITEM_LOG
    """
  }

  def bestOrdereAmtList() : String = {
    """
		SELECT
           itemId
          ,ROW_NUMBER() OVER (ORDER BY purchaseAmt*openWeight DESC) orderAmtRank
    FROM
          EACH_ITEM_LOG
    """
  }

  def bestClickCountList() : String = {
    """
		SELECT
           itemId
          ,ROW_NUMBER() OVER (ORDER BY clickCount*openWeight DESC) clickRank
    FROM
          EACH_ITEM_LOG
    """
  }


  def eachItemLog() : String = {
    """
    SELECT
           A.itemId
          ,(CASE WHEN A.purchaseMbrId IS null THEN 0.0001 ELSE A.purchaseMbrId END) purchaseMbrId
          ,(CASE WHEN A.purchaseCount IS null THEN 0.0001 ELSE A.purchaseCount END) purchaseCount
          ,(CASE WHEN A.purchaseAmt IS null THEN 0.0001 ELSE A.purchaseAmt END)     purchaseAmt
          ,(CASE
              WHEN  B.clickDistinctPcid < A.purchaseMbrId
              THEN  B.clickDistinctPcid + A.purchaseMbrId
              ELSE  B.clickDistinctPcid END
           ) clickMbrId
          ,(CASE
              WHEN B.clickCount < A.purchaseCount
              THEN B.clickCount + A.purchaseCount
              ELSE B.clickCount END
          ) clickCount
          ,A.siteNo
          ,(CASE
              WHEN C.openWeight IS NOT NULL AND C.openWeight < 1/7
              THEN ROUND(1/7,4)
              WHEN C.openWeight IS NULL
              THEN ROUND(1/7,4)
              ELSE C.openWeight END
          ) openWeight
          ,C.dispStrtDts
    FROM
          EACH_ITEM_BUY_LOG A
    INNER JOIN
          EACH_ITEM_CLICK_LOG B
    ON    A.itemId = B.itemId
    INNER JOIN
          SITE_UNIQ_ITEM_LIST C
    ON    A.itemId = C.itemId
    """
  }


  def eachItemBuyLog() : String = {
    """
    SELECT
           A.itemId
          ,max(purchaseMbrId)   purchaseMbrId
          ,max(purchaseCount)   purchaseCount
          ,max(purchaseAmt)     purchaseAmt
          ,max(siteNo)          siteNo
    FROM  (
                SELECT
                       A.itemId
                      ,count(distinct A.mbrId)  purchaseMbrId
                      ,count(*)                 purchaseCount
                      ,sum(B.price)             purchaseAmt
                      ,max(B.siteNo)            siteNo
                FROM
                      CURRENT_ITEM_BUY_LOG A
                INNER JOIN
                      SITE_UNIQ_ITEM_LIST B
                ON    A.itemId = B.itemId
                GROUP BY
                      A.itemId

                UNION

            		SELECT
                       A.itemId
                      ,0.0001                 purchaseMbrId
                      ,0.0001                 purchaseCount
                      ,0.0001                 purchaseAmt
                      ,max(B.siteNo)          siteNo
                FROM
                      CURRENT_CLICK_LOG A
                INNER JOIN
                      SITE_UNIQ_ITEM_LIST B
                ON    A.itemId = B.itemId
                GROUP BY
                      A.itemId
    ) A
    GROUP BY
          itemId
    """
  }


  def eachItemClickLog() : String = {
    """
		SELECT
           itemId
          ,count(distinct pcid) clickDistinctPcid
          ,count(*)             clickCount
          ,max(siteNo)          siteNo
    FROM
          CURRENT_CLICK_LOG
    GROUP BY
          itemId
    """
  }

  def currentClickLog() : String = {
    """
		SELECT
		         A.itemId
            ,A.pcid
            ,A.siteNo
		FROM
            PRE_TRACK A
    INNER JOIN
            FULL_ITEM_LIST B
    ON
            A.itemId = B.itemId
    INNER JOIN
            UPDATE_LOGIN_HISTORY C
    ON
            A.pcid = C.pcid
    """
  }


  def fullItemList() : String = {
    """
			SELECT
						A.itemId
      		, C.siteNo
					, C.sellPrc
          , C.lwstSellPrc
          , ROUND(1/datediff(current_date,date(SUBSTR(A.dispStrtDts,0,10))),4) openWeight
          , A.dispStrtDts
			FROM
          PRE_ITEM A
			INNER JOIN
			    ITEM_PRICE C
			ON
			    A.itemId = C.itemId
			WHERE
			        sellStatCd = 20
          and (case when itemSellTypeCd in (10, 20) then true
		          when itemSellTypeCd =30 and itemSellTypeDtlCd = 70 then true
		          else false end)
    """
  }


  def siteUniqItemList() : String = {
    """
			SELECT
						itemId
      		, siteNo
					, MIN(lwstSellPrc) price
          , MAX(openWeight) openWeight
          , MIN(dispStrtDts) dispStrtDts
			FROM
          FULL_ITEM_LIST
			GROUP BY
           itemId
          ,siteNo
    """
  }

  def currentItemBuyLog() : String = {
    """
		SELECT
		         B.itemId
            ,A.siteNo
            ,A.mbrId
		FROM
            ITEM_BUY_LOG A
    INNER JOIN
            FULL_ITEM_LIST B
    ON
            A.itemId = B.itemId
    AND     A.siteNo = B.siteNo

    """
  }

  def itemBuyLog() : String = {
    """
		SELECT
		         itemId
            ,siteNo
            ,mbrId
		FROM
            ORD_ITEM
    WHERE   1=1
    AND     datediff(current_date, date(ordDt)) <= 8
    """
  }

  def everyClick() : String = {
    """
		SELECT
             visitDts
            ,ip
            ,siteNo
            ,itemId
            ,pcid
            ,mbrId
		FROM
            TRACKING_LOG_TEMP
    """
  }

}
