package ssg.searchbesteatype

/**
  * Created by moneymall on 16/05/11.
  */

/**
  * Created by reated by Sung Ryu on 16/05/11.
  */



object SQLMaker {

  // MA의 검색->상품상세 영역코드
  val tareaSearchItemDetail =
    """
            tarea LIKE '검색결과|전체상품탭|특가딜영역_상품_클릭%%'
         OR tarea LIKE '검색결과|전체상품탭|상품_클릭%%'
         OR tarea LIKE '검색결과|테마검색탭|이슈테마레이어_상품클릭%%'
         OR tarea LIKE '검색결과|도서취미검색결과|상품_클릭%%'
    """

  //기여매장정의셋의 상품상세URL
  //170712 갱신
  val itemDetailURL = """
         url LIKE '%%burberryItemView.ssg%%'
      OR url LIKE '%%chanelItemDetail.ssg%%'
      OR url LIKE '%%dealItemView.ssg%%'
      OR url LIKE '%%gucciItemDtl.ssg%%'
      OR url LIKE '%%iframeBbrItemDtlDesc.ssg%%'
      OR url LIKE '%%itemDtl.ssg%%'
      OR url LIKE '%%itemList.ssg%%'
      OR url LIKE '%%itemView.ssg%%'
      OR url LIKE '%%itemView01.ssg%%'
      OR url LIKE '%%macItemView.ssg%%'
  """

  //MA네이티브 상품상세 Deplth1 코드
  val itemDetailDepth1Code =
    """
          googleCode LIKE '%%ItemDetail'
       OR googleCode LIKE '%%ItemDetailViewController'
       OR googleCode LIKE '%%DealViewController'
       OR googleCode LIKE '%%ProductDetailNativeFragment'
       OR googleCode LIKE '%%ProductDealNativeFragment'

    """


  def searchItemDetailClickFromMA_q() : String = {
    """
        SELECT
              itemId
              ,count(*) AS cnt
              ,'MA' AS Type
        FROM reactingEventD1D8
        WHERE
            ( """ + tareaSearchItemDetail + """)
        AND deviceType = "MA"
        AND %s
        GROUP BY itemId
    """
  }



  def searchItemDetailClickFromPCMW_q() : String = {
    """
        SELECT
               itemId
               ,COUNT(*) AS cnt
               ,'%s' AS Type
        FROM
                everyClickD1D8
        WHERE
                (""" + itemDetailURL + """)
        AND     referrer like '%%search.ssg%%'
        AND     deviceType = '%s'
        AND     %s
        GROUP BY itemId
    """
  }


  def searchCartClickFromMA_q() : String = {

    """
        SELECT
              itemId
              ,COUNT(*) AS cnt
              ,'MA-CART' AS Type
        FROM contSales
        WHERE
                  deviceType = 'MA'
              AND (
                    (googleCode LIKE '%%SearchResult' or googleCode LIKE '%%ProductListFragment' or googleCode LIKE '%%NewSearchResultBaseViewController') OR  (url LIKE '%%search.ssg%%' OR url LIKE '%%jsonSearch.ssg%%')
                  )
              AND %s
        GROUP BY itemId

    """
  }

  def searchCartClickFromPCMW_q() : String = {
    """
        SELECT
              itemId
              ,COUNT(*) AS cnt
              ,'%s' AS Type
        FROM contSales
        WHERE
                  deviceType = '%s'
              AND (url LIKE '%%search.ssg%%' OR url LIKE '%%jsonSearch.ssg%%')
              AND %s
        GROUP BY itemId
    """
  }


  def sumByItem_q() : String = {
    """
        SELECT
                itemId
                ,SUM(cnt) AS %s
        FROM  %s
        GROUP BY itemId
    """
  }

  def sumByItemMinusSearchClick_q() : String = {
    """
      SELECT
            A.itemId
            ,CASE
              WHEN
                  A.cnt - NVL(B.%s,0) < 0 THEN 0
              ELSE
                  A.cnt - NVL(B.%s,0)
            END AS %s
      FROM (
          SELECT
                  itemId
                  ,SUM(cnt) AS cnt
          FROM %s
          GROUP BY itemId
      ) A
      LEFT JOIN %s B
      ON A.itemId = B.itemId
    """
  }



  def totalCnt_q() : String = {
    """
        SELECT
              SUM(cnt) AS cnt
        FROM %s
    """
  }


  def srchFrqIncrRt_q() : String = {
    """
        SELECT
              NVL(A.itemId,B.itemId) itemId
              ,((( NVL(cast(A.cnt AS INT),0) / CAST(%s AS BIGINT) ) - (NVL(CAST(B.cnt AS INT),0) / CAST(%s AS BIGINT))) / 7) * 10000000 AS srchFrqIncrRt
        FROM searchClickD1 A
        FULL OUTER JOIN searchClickD8 B
        ON A.itemId = B.itemId
    """
  }


  def itemDetailClickFromMA_q() : String = {
    """
        SELECT
                itemId
                ,COUNT(*) AS cnt
                ,'MA' AS Type
        FROM everyClickD1D8
        WHERE
                (""" + itemDetailDepth1Code + """)
            AND deviceType = 'MA'
            AND %s
        GROUP BY itemId
    """
  }

  def itemDetailClickFromMAMWPC_q() : String = {
    """
        SELECT
              itemId
              ,COUNT(*) AS cnt
              ,'%s' AS Type
        FROM
            everyClickD1D8
        WHERE
                (""" + itemDetailURL + """)
                AND deviceType = '%s'
                AND %s
        GROUP BY itemId
    """
  }


  def cartClickFromAllDevice_q() : String = {
    """
        SELECT
                itemId
                ,COUNT(*) AS cnt
                ,'ALL-CART' AS Type
        FROM contSales
        WHERE
            timeStamp = '%s'
        GROUP BY itemId
    """
  }


  def cartClick3Days_q() : String = {
    """
        SELECT
              itemId
              ,COUNT(*) AS totalCartCnt
        FROM contSales
        WHERE
            timeStamp >= '%s'
        GROUP BY itemId
    """
  }


  def clickFrqIncrRt_q() : String = {
    """
        SELECT
              NVL(A.itemId,B.itemId) itemId
              ,(((NVL(cast(A.cnt AS INT),0) / CAST(%s AS BIGINT)) - (NVL(CAST(B.cnt AS INT),0) / CAST(%s AS BIGINT))) / 7) * 100000000 AS clickFrqIncrRt
        FROM itemClickD1 A
        FULL OUTER JOIN itemClickD8 B
        ON A.itemId = B.itemId
    """
  }




  def itemReply_q() : String = {
    """
          SELECT
                itemId
                ,replyCnt
          FROM itemReplyTemp
    """
  }


  def ordInfo_q() : String = {
    """
          SELECT
              itemId
              ,ordFrq
              ,rlordAmt
              ,uPrc
              ,itemDcRt
          FROM (
                SELECT
                    itemId
                    ,COUNT(DISTINCT(orordNo)) AS ordFrq
                    ,SUM(ordAmt-owncoBdnItemDcAmt-coopcoBdnItemDcAmt) AS rlordAmt
                    ,SUM(ordAmt-owncoBdnItemDcAmt-coopcoBdnItemDcAmt) / SUM(ordQty) AS uPrc
                    ,CASE
                        WHEN (SUM(owncoBdnItemDcAmt+coopcoBdnItemDcAmt)/SUM(ordAmt)) < 0 THEN 0
                        ELSE SUM(owncoBdnItemDcAmt+coopcoBdnItemDcAmt)/SUM(ordAmt)
                     END AS itemDcRt
                    ,SUM(ordQty) AS ordQty
                FROM ordItem
                WHERE ordDt >= '%s'
                AND ordAplTgtCd = '1'
                GROUP BY itemId) A
          WHERE
                    rlordAmt > 0
                AND ordQty > 0
    """
  }



  def mergeTable_q() : String = {
    """
       SELECT
               A.itemId
               ,A.clickFrqIncrRt
               ,A.srchFrqIncrRt
               ,A.totalSrchCnt
               ,A.ordFrq
               ,A.totalCartCnt
               ,A.replyCnt
               ,A.rlordAmt
               ,NVL(A.uPrc,H.sellPrc) AS uPrc
               ,A.itemDcRt
               ,A.totalItemClickCnt
               ,A.emartOffOrdFrq
        FROM (
                SELECT
                        A.itemId
                        ,NVL(D.clickFrqIncrRt,0) AS clickFrqIncrRt
                        ,NVL(B.srchFrqIncrRt,0) AS srchFrqIncrRt
                        ,NVL(C.totalSrchCnt,0) AS totalSrchCnt
                        ,NVL(G.ordFrq,0) AS ordFrq
                        ,NVL(E.totalCartCnt,0) AS totalCartCnt
                        ,NVL(F.replyCnt,0) AS replyCnt
                        ,NVL(G.rlordAmt,0) AS rlordAmt
                        ,G.uPrc
                        ,NVL(G.itemDcRt,0) AS itemDcRt
                        ,NVL(H.totalItemClickCnt,0) AS totalItemClickCnt
                        ,NVL(I.emartOffOrdFrq, 0) AS emartOffOrdFrq
                FROM   distinctItem A
                FULL OUTER JOIN srchFrqIncrRt B ON A.itemId = B.itemId
                FULL OUTER JOIN searchClick3Days C ON A.itemId = C.itemId
                FULL OUTER JOIN clickFrqIncrRt D ON A.itemId = D.itemId
                FULL OUTER JOIN cartClick3Days E ON A.itemId = E.itemId
                FULL OUTER JOIN itemReply F ON A.itemId = F.itemId
                FULL OUTER JOIN ordInfo G ON A.itemId = G.itemId
                FULL OUTER JOIN itemClick3Days H ON A.itemId = H.itemId
                FULL OUTER JOIN emartOffOrdInfo I ON A.itemId = I.itemId
                ) A
        INNER JOIN ( SELECT
                            itemId
                            ,MAX(CAST(sellPrc AS bigint)) AS sellPrc
                     FROM
                            itemPrc
                     GROUP BY
                            itemId
                  ) H
        ON A.itemId = H.itemId

    """
  }

  def getDistcintItemId() : String = {
    """
      SELECT
		          DISTINCT(itemId) AS itemId
      FROM %s
    """
  }

  def filterMergeTable_q() : String = {
    """
        SELECT
                itemId
                ,clickFrqIncrRt
                ,srchFrqIncrRt
                ,CASE
                     WHEN A.diff >= 0 AND A.diff <= 7 THEN 1/1
                     WHEN A.diff > 7 AND A.diff <= 14 THEN 1/2
                     WHEN A.diff > 14 AND A.diff <= 21 THEN 1/3
                     WHEN A.diff > 21 AND A.diff <= 28 THEN 1/4
                     WHEN A.diff > 28 AND A.diff <= 35 THEN 1/5
                     WHEN A.diff > 35 AND A.diff <= 42 THEN 1/6
                     WHEN A.diff > 42 AND A.diff <= 49 THEN 1/7
                     WHEN A.diff > 49 AND A.diff <= 56 THEN 1/8
                     WHEN A.diff > 56 AND A.diff <= 63 THEN 1/9
                     WHEN A.diff > 63 AND A.diff <= 70 THEN 1/10
                     WHEN A.diff > 70 AND A.diff <= 77 THEN 1/11
                     WHEN A.diff > 77 AND A.diff <= 84 THEN 1/12
                     ELSE 1/12
                 END itemRegIndex
                ,totalSrchCnt
                ,totalItemClickCnt
                ,emartOffOrdFrq
                ,ordFrq
                ,totalCartCnt
                ,replyCnt
                ,rlordAmt
                ,uPrc
                ,itemDcRt
                ,brandId
                ,stdCtgId
                ,dispStrtDts
                ,nitmAplYn
        FROM (
                SELECT
                         A.itemId
                        ,A.clickFrqIncrRt
                        ,A.srchFrqIncrRt
                        ,A.totalSrchCnt
                        ,A.ordFrq
                        ,A.totalCartCnt
                        ,A.replyCnt
                        ,A.rlordAmt
                        ,A.uPrc
                        ,A.itemDcRt
                        ,A.totalItemClickCnt
                        ,A.emartOffOrdFrq
                        ,DATEDIFF(CURRENT_DATE,DATE(SUBSTR(B.dispStrtDts,0,10))) AS diff
                        ,B.brandId
                        ,B.stdCtgId
                        ,B.dispStrtDts
                        ,B.nitmAplYn
                FROM mergeTable A
                INNER JOIN item B ON A.itemId = B.itemId
        ) A
    """
  }





  def logScaleTable_q() : String = {
    """
        SELECT
                itemId
                ,NVL(LOG10(clickFrqIncrRt),0) clickFrqIncrRt
                ,NVL(LOG10(srchFrqIncrRt),0) srchFrqIncrRt
                ,itemRegIndex
                ,NVL(LOG10(totalSrchCnt),0) totalSrchCnt
                ,NVL(LOG10(totalItemClickCnt ),0) totalItemClickCnt
                ,NVL(LOG10(ordFrq),0) ordFrq
                ,NVL(LOG10(totalCartCnt),0) totalCartCnt
                ,NVL(LOG10(replyCnt),0) replyCnt
                ,NVL(LOG10(rlordAmt),0) rlordAmt
                ,NVL(LOG10(uPrc),0) uPrc
                ,NVL(LOG10(emartOffOrdFrq),0) emartOffOrdFrq
                ,itemDcRt
                ,brandId
                ,stdCtgId
                ,dispStrtDts
                ,nitmAplYn
				        ,clickFrqIncrRt raw_clickFrqIncrRt
                ,srchFrqIncrRt raw_srchFrqIncrRt
                ,totalSrchCnt raw_totalSrchCnt
                ,totalItemClickCnt raw_totalItemClickCnt
                ,ordFrq raw_ordFrq
                ,totalCartCnt raw_totalCartCnt
                ,replyCnt raw_replyCnt
                ,rlordAmt raw_rlordAmt
                ,uPrc raw_uPrc
                ,emartOffOrdFrq raw_emartOffOrdFrq
        FROM
                filterMergeTable
    """
  }



  def normalization_var_q() : String = {
    """
        SELECT
             MAX(CAST(clickFrqIncrRt AS FLOAT)) AS maxClickFrqIncrRt
            ,MIN(CAST(clickFrqIncrRt AS FLOAT)) AS minClickFrqIncrRt

            ,MAX(CAST(srchFrqIncrRt AS FLOAT)) AS maxSrchFrqIncrRt
            ,MIN(CAST(srchFrqIncrRt AS FLOAT)) AS minSrchFrqIncrRt

            ,MAX(CAST(itemRegIndex AS FLOAT)) AS maxItemRegIndex
            ,MIN(CAST(itemRegIndex AS FLOAT)) AS minItemRegIndex

            ,MAX(CAST(totalSrchCnt AS FLOAT)) AS maxTotalSrchCnt
            ,MIN(CAST(totalSrchCnt AS FLOAT)) AS minTotalSrchCnt

            ,MAX(CAST(totalItemClickCnt AS FLOAT)) AS maxTotalItemClickCnt
            ,MIN(CAST(totalItemClickCnt AS FLOAT)) AS minTotalItemClickCnt

            ,MAX(CAST(ordFrq AS FLOAT)) AS maxOrdFrq
            ,MIN(CAST(ordFrq AS FLOAT)) AS minOrdFrq

            ,MAX(CAST(totalCartCnt AS FLOAT)) AS maxTotalCartCnt
            ,MIN(CAST(totalCartCnt AS FLOAT)) AS minTotalCartCnt

            ,MAX(CAST(replyCnt AS FLOAT)) AS maxReplyCnt
            ,MIN(CAST(replyCnt AS FLOAT)) AS minReplyCnt

            ,MAX(CAST(rlordAmt AS FLOAT)) AS maxRlordAmt
            ,MIN(CAST(rlordAmt AS FLOAT)) AS minRlordAmt

            ,MAX(CAST(uPrc AS FLOAT)) AS maxUPrc
            ,MIN(CAST(uPrc AS FLOAT)) AS minUPrc

            ,MAX(CAST(itemDcRt AS FLOAT)) AS maxItemDcRt
            ,MIN(CAST(itemDcRt AS FLOAT)) AS minItemDcRt

            ,MAX(CAST(emartOffOrdFrq AS FLOAT)) AS maxEmartOffOrdFrq
            ,MIN(CAST(emartOffOrdFrq AS FLOAT)) AS minEmartOffOrdFrq

        FROM logScaleTable
    """
  }

  def normalization_q() : String = {
    """
        SELECT
              itemId
              ,100 * ((CAST(clickFrqIncrRt AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) clickFrqIncrRtNormal
              ,100 * ((CAST(srchFrqIncrRt AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) srchFrqIncrRtNormal
              ,100 * ((CAST(itemRegIndex AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) itemRegIndexNormal
              ,100 * ((CAST(totalSrchCnt AS FLOAT)  - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) totalSrchCntNormal
              ,100 * ((CAST(totalItemClickCnt AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) totalItemClickCntNormal
              ,100 * ((CAST(ordFrq AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) ordFrqNormal
              ,100 * ((CAST(totalCartCnt AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) totalCartCntNormal
              ,100 * ((CAST(replyCnt AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) replyCntNormal
              ,100 * ((CAST(rlordAmt AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) rlordAmtNormal
              ,100 - (100 * ((CAST(uPrc AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT))) uPrcNormal
              ,100 * ((CAST(itemDcRt AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) itemDcRtNormal
              ,100 * ((CAST(emartOffOrdFrq AS FLOAT) - CAST(%s AS FLOAT)) / CAST(%s AS FLOAT)) emartOffOrdFrqNormal
              ,clickFrqIncrRt
              ,srchFrqIncrRt
              ,itemRegIndex
              ,totalSrchCnt
              ,totalItemClickCnt
              ,ordFrq
              ,totalCartCnt
              ,replyCnt
              ,rlordAmt
              ,uPrc
              ,itemDcRt
              ,brandId
              ,stdCtgId
              ,dispStrtDts
              ,nitmAplYn
              ,raw_clickFrqIncrRt
              ,raw_srchFrqIncrRt
              ,raw_totalSrchCnt
              ,raw_totalItemClickCnt
              ,raw_ordFrq
              ,raw_totalCartCnt
              ,raw_replyCnt
              ,raw_rlordAmt
              ,raw_uPrc
              ,raw_emartOffOrdFrq
        FROM logScaleTable A
    """
  }

  def ranking_q() : String = {
    """
        SELECT
             DATE_FORMAT(CURRENT_DATE,'YYYYMMdd') CRITN_DT
            ,itemId ITEM_ID
            ,(
              (CAST(0.2 AS FLOAT) * (CAST(srchFrqIncrRtNormal AS FLOAT)*0.4 + CAST(clickFrqIncrRtNormal AS FLOAT)*0.4 + CAST(itemRegIndexNormal AS FLOAT)*0.2))
            + (CAST(0.5 AS FLOAT) * (CAST(totalCartCntNormal AS FLOAT)*0.25 + CAST(ordFrqNormal AS FLOAT)*0.2 + CAST(replyCntNormal AS FLOAT)*0.2 + CAST(totalSrchCntNormal AS FLOAT)*0.15 + CAST(totalItemClickCntNormal AS FLOAT)*0.1 + CAST(rlordAmtNormal AS FLOAT)*0.1))
            + (CAST(0.3 AS FLOAT) * (CAST(uPrcNormal AS FLOAT)*0.6 + CAST(itemDcRtNormal AS FLOAT)*0.4))
            ) AS SRCH_TYPE_FIRS_SCR
            ,(
              (CAST(0.5 AS FLOAT) * (CAST(srchFrqIncrRtNormal AS FLOAT)*0.4 + CAST(clickFrqIncrRtNormal AS FLOAT)*0.4 + CAST(itemRegIndexNormal AS FLOAT)*0.2))
            + (CAST(0.2 AS FLOAT) * (CAST(totalCartCntNormal AS FLOAT)*0.25 + CAST(ordFrqNormal AS FLOAT)*0.2 + CAST(replyCntNormal AS FLOAT)*0.2 + CAST(totalSrchCntNormal AS FLOAT)*0.15 + CAST(totalItemClickCntNormal AS FLOAT)*0.1 + CAST(rlordAmtNormal AS FLOAT)*0.1))
            + (CAST(0.3 AS FLOAT) * (CAST(uPrcNormal AS FLOAT)*0.6 + CAST(itemDcRtNormal AS FLOAT)*0.4))
            ) AS SRCH_TYPE_SCND_SCR
            ,(
              (CAST(0.3 AS FLOAT) * (CAST(srchFrqIncrRtNormal AS FLOAT)*0.4 + CAST(clickFrqIncrRtNormal AS FLOAT)*0.4 + CAST(itemRegIndexNormal AS FLOAT)*0.2))
            + (CAST(0.5 AS FLOAT) * (CAST(totalCartCntNormal AS FLOAT)*0.25 + CAST(ordFrqNormal AS FLOAT)*0.2 + CAST(replyCntNormal AS FLOAT)*0.2 + CAST(totalSrchCntNormal AS FLOAT)*0.15 + CAST(totalItemClickCntNormal AS FLOAT)*0.1 + CAST(rlordAmtNormal AS FLOAT)*0.1))
            + (CAST(0.2 AS FLOAT) * (CAST(uPrcNormal AS FLOAT)*0.6 + CAST(itemDcRtNormal AS FLOAT)*0.4))
            ) AS SRCH_TYPE_THRD_SCR
            ,clickFrqIncrRt CLICK_ICRT
            ,srchFrqIncrRt SRCH_ICRT
            ,itemRegIndex NEW_ITEM_REG_SCR
            ,raw_totalSrchCnt SRCH_CNT
            ,raw_totalItemClickCnt CLICK_CNT
            ,raw_totalCartCnt CART_ITEM_QTY
            ,raw_ordFrq ORD_QTY
            ,raw_rlordAmt RLORD_AMT
            ,raw_uPrc RLORD_UPRC
            ,itemDcRt ITEM_DCRT
            ,raw_replyCnt RECOM_CONT
            ,(

              CAST(totalCartCntNormal AS FLOAT)*0.15 + CAST(ordFrqNormal AS FLOAT)*0.35 + CAST(replyCntNormal AS FLOAT)*0.25 + CAST(totalSrchCntNormal AS FLOAT)*0.05 + CAST(emartOffOrdFrqNormal AS FLOAT)*0.1 + CAST(rlordAmtNormal AS FLOAT)*0.1


            ) AS SRCH_TYPE_FRTH_SCR
            ,raw_emartOffOrdFrq STR_ORD_QTY
        FROM normalizationFilterMergeTable
    """
  }


    def ranking_debug_q() : String = {
      """
        SELECT
             DATE_FORMAT(CURRENT_DATE,'YYYYMMdd') CRITN_DT
            ,itemId ITEM_ID
            ,(
              (CAST(0.2 AS FLOAT) * (CAST(srchFrqIncrRtNormal AS FLOAT)*0.4 + CAST(clickFrqIncrRtNormal AS FLOAT)*0.4 + CAST(itemRegIndexNormal AS FLOAT)*0.2))
            + (CAST(0.5 AS FLOAT) * (CAST(totalCartCntNormal AS FLOAT)*0.25 + CAST(ordFrqNormal AS FLOAT)*0.2 + CAST(replyCntNormal AS FLOAT)*0.2 + CAST(totalSrchCntNormal AS FLOAT)*0.15 + CAST(totalItemClickCntNormal AS FLOAT)*0.1 + CAST(rlordAmtNormal AS FLOAT)*0.1))
            + (CAST(0.3 AS FLOAT) * (CAST(uPrcNormal AS FLOAT)*0.6 + CAST(itemDcRtNormal AS FLOAT)*0.4))
            ) AS SRCH_TYPE_FIRS_SCR
            ,(
              (CAST(0.5 AS FLOAT) * (CAST(srchFrqIncrRtNormal AS FLOAT)*0.4 + CAST(clickFrqIncrRtNormal AS FLOAT)*0.4 + CAST(itemRegIndexNormal AS FLOAT)*0.2))
            + (CAST(0.2 AS FLOAT) * (CAST(totalCartCntNormal AS FLOAT)*0.25 + CAST(ordFrqNormal AS FLOAT)*0.2 + CAST(replyCntNormal AS FLOAT)*0.2 + CAST(totalSrchCntNormal AS FLOAT)*0.15 + CAST(totalItemClickCntNormal AS FLOAT)*0.1 + CAST(rlordAmtNormal AS FLOAT)*0.1))
            + (CAST(0.3 AS FLOAT) * (CAST(uPrcNormal AS FLOAT)*0.6 + CAST(itemDcRtNormal AS FLOAT)*0.4))
            ) AS SRCH_TYPE_SCND_SCR
            ,(
              (CAST(0.3 AS FLOAT) * (CAST(srchFrqIncrRtNormal AS FLOAT)*0.4 + CAST(clickFrqIncrRtNormal AS FLOAT)*0.4 + CAST(itemRegIndexNormal AS FLOAT)*0.2))
            + (CAST(0.5 AS FLOAT) * (CAST(totalCartCntNormal AS FLOAT)*0.25 + CAST(ordFrqNormal AS FLOAT)*0.2 + CAST(replyCntNormal AS FLOAT)*0.2 + CAST(totalSrchCntNormal AS FLOAT)*0.15 + CAST(totalItemClickCntNormal AS FLOAT)*0.1 + CAST(rlordAmtNormal AS FLOAT)*0.1))
            + (CAST(0.2 AS FLOAT) * (CAST(uPrcNormal AS FLOAT)*0.6 + CAST(itemDcRtNormal AS FLOAT)*0.4))
            ) AS SRCH_TYPE_THRD_SCR
            ,clickFrqIncrRt CLICK_ICRT
            ,srchFrqIncrRt SRCH_ICRT
            ,itemRegIndex NEW_ITEM_REG_SCR
            ,totalSrchCnt SRCH_CNT
            ,totalItemClickCnt CLICK_CNT
            ,totalCartCnt CART_ITEM_QTY
            ,ordFrq ORD_QTY
            ,rlordAmt RLORD_AMT
            ,uPrc RLORD_UPRC
            ,itemDcRt ITEM_DCRT
            ,replyCnt RECOM_CONT
            ,srchFrqIncrRtNormal
            ,clickFrqIncrRtNormal
            ,itemRegIndexNormal
            ,totalCartCntNormal
            ,ordFrqNormal
            ,replyCntNormal
            ,totalSrchCntNormal
            ,totalItemClickCntNormal
            ,rlordAmtNormal
            ,uPrcNormal
            ,itemDcRtNormal
            ,raw_clickFrqIncrRt RAW_CLICK_ICRT
            ,raw_srchFrqIncrRt RAW_SRCH_ICRT
            ,raw_totalSrchCnt RAW_SRCH_CNT
            ,raw_totalItemClickCnt RAW_CLICK_CNT
            ,raw_totalCartCnt RAW_CART_ITEM_QTY
            ,raw_ordFrq RAW_ORD_QTY
            ,raw_rlordAmt RAW_RLORD_AMT
            ,raw_uPrc RAW_RLORD_UPRC
            ,raw_replyCnt RAW_RECOM_CONT
            ,(

              CAST(totalCartCntNormal AS FLOAT)*0.15 + CAST(ordFrqNormal AS FLOAT)*0.35 + CAST(replyCntNormal AS FLOAT)*0.25 + CAST(totalSrchCntNormal AS FLOAT)*0.05 + CAST(emartOffOrdFrqNormal AS FLOAT)*0.1 + CAST(rlordAmtNormal AS FLOAT)*0.1

            ) AS SRCH_TYPE_FRTH_SCR
            ,raw_emartOffOrdFrq STR_ORD_QTY
        FROM normalizationFilterMergeTable
      """
  }


  def itemRanking() : String = {
    """
        SELECT
             A.item_id
             ,A.nitmAplYn
             ,B.siteNo
             ,A.stdCtgId
             ,A.brandId
             ,A.SRCH_TYPE_FIRS_SCR
             ,A.SRCH_TYPE_SCND_SCR
             ,A.SRCH_TYPE_THRD_SCR
             ,A.dispStrtDts
             ,A.critn_dt
        FROM
             ranking A
        INNER JOIN  (
              SELECT
                  itemId
                  ,siteNo
              FROM
                  itemPrc
              GROUP BY
                  itemId
                  ,siteNo
        ) B
        ON
            A.item_id = B.itemId
    """
  }

  //20170426
  //유성
  //미사용
  def getVer4ItemId() : String = {
    """
            select itemId
            from item a
            join STD_CTG_KEY_PATH b
            on a.stdCtgId = b.std_Ctg_id
            where
                      b.STD_CTG_LCLS_ID = '1000000007'
                  or  b.STD_CTG_MCLS_ID = '1000020864'
                  or  b.STD_CTG_MCLS_ID = '2000000017'
                  or  b.STD_CTG_MCLS_ID = '2000000019'
                  or  b.STD_CTG_MCLS_ID = '2000000020'
                  or  b.STD_CTG_MCLS_ID = '2000000021'
                  or  b.STD_CTG_MCLS_ID = '2000000022'
                  or  b.STD_CTG_MCLS_ID = '2000000023'
                  or  b.STD_CTG_MCLS_ID = '2000000024'
                  or  b.STD_CTG_MCLS_ID = '2000000025'
                  or  b.STD_CTG_MCLS_ID = '2000000026'
                  or  b.STD_CTG_MCLS_ID = '2000000027'
                  or  b.STD_CTG_MCLS_ID = '2000000028'
    """
  }

  def getEmartOffOrdFrq() : String = {
    """
        SELECT
            itemId,
            count(*) emartOffOrdFrq
        FROM (
            SELECT
                  sale_date,
                  I.ITEM_ID itemId
            FROM EMART_OFF_SALE_LIST A
            LEFT JOIN OFFLINE_ITEM_MPNG I
            ON  I.BIZCND_CD       = '10'
            AND A.ITEM_ID         = I.OFFLINE_ITEM_ID
            WHERE
                I.ITEM_ID is not null
        ) A group by itemId
    """
  }

}
