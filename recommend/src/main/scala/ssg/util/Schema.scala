package ssg.util

/**
  * Created by Y.G 26/11/15.
  */


import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.util.UUID

case class TrackItemDtl(visitDts:String, ip:String, siteNo:String, itemId:String, pcid:String, mbrId:String, fsid:String, timestamp:String)
case class Item(itemId:String, mblNm:String, splVenId:String, brandId:String, sellStatCd:String, prodManufCntryId:String, stdCtgId:String, itemSellTypeCd:String, itemSellTypeDtlCd:String, dispStrtDts:String, regDts:String, nitmAplYn:String, srchPsblYn:String)
case class ItemEvent(itemId:String)
case class ItemPrice(itemId:String, siteNo:String, salestrNo:String, sellPrc:String, lwstSellPrc:String)
case class OrdItem(orordNo:String, ordNo:String, ordItemDivCd:String, ordDt:String, dvicDivCd:String, ordItemStatCd:String, itemId:String,siteNo:String, mbrId:String, sellPrc:String,  splPrc:String, orordQty:String, orordAmt:String, stdCtgId:String, brandId:String, infloSiteNo:String, splVenId:String, regDts:String, ordAmt:Long, owncoBdnItemDcAmt:Long, coopcoBdnItemDcAmt:Long)
case class CartView(visitDts:String, ip:String, siteNo:String, itemId:String, pcid:String, mbrId:String)
case class LoginHistory(mbrId:String, pcid:String, updateDate:String)
case class StdCtgUTF8(stdCtgId:String, stdCtgNm:String, priorStdCtgId:String, stdCtgLclsId:String, stdCtgLclsNm:String, stdCtgMclsId:String, stdCtgMclsNm:String, stdCtgSclsId:String, stdCtgSclsNm:String, stdCtgDclsId:String, stdCtgDclsNm:String, useYn:String)
case class StdCtg(stdCtgId:String, priorStdCtgId:String, stdCtgLclsId:String, stdCtgMclsId:String, stdCtgSclsId:String, stdCtgDclsId:String, useYn:String)
case class OrderEventRow(ordNo:String, itemId:String, timestamp:String, mbrId:Long)
case class ItemBased(itemId:String, recommendedItemId:String, score:Double)
case class CategorizedItemBased(itemId:String, stdCtg:String, recommendItemId:String, siteNo:String, recommendStdCtg:String, score:Double, method:String)
case class Collaboration(userId:String, itemId:String, cnt:Long, timestamp:String)
case class CassandraItemBased(itemId:String, recommendItemId:String, score:String, method:String, siteNo:String, regDts:UUID)
case class Brand(brandId:String, brandNm:String, useYn:String, brandRegMainDivCd:String)
case class BestRank(itemId:String, siteNo:String, dispSiteNo:String, ordCnt:String, ordCntRank:String, brandId:String, trkItemCnt:String, trkItemCntRank:String, sellPrc:String, sellPrcRank:String, totalScore:String, totalRank:String, finalRank:String)
case class BrandBestData(brandId:String, brandNm:String, itemId:String, score:Double, regDts:UUID)
case class ItemMasters(itemId:String, brandId:String, stdCtgId:String)
case class CategoryBestData(stdCtgId:String, stdCtgNm:String, itemId:String, score:Double, regDts:UUID)
case class SearchBestCaseModel(itemId:String, firstScore:String, secondScore:String, thirdScore:String)
case class TableDivision(div:String,code:String)
case class Itembase(itemId:String, siteNO:String, recomItemId:String,score:String)
case class ClipItem(brandId:String, kind:String, id:String, rank:String)

case class Itembase2(itemId:String, siteNo:String, method:String, recomItemId:String,regDts:Option[UUID], score:Double)



//유형별검색베스트

case class OrdItem2(
                     orordNo:String,
                     ordDt:String,
                     itemId:String,
                     ordAmt:Long,
                     owncoBdnItemDcAmt:Long,
                     coopcoBdnItemDcAmt:Long,
                     ordQty:Integer,
                     ordAplTgtCd:String)


case class itemReplySch(
                         itemId:String,
                         replyCnt:Integer
                       )

case class contSalesSch(
                         timeStamp:String,
                         url:String,
                         domainSiteNo:String,
                         deviceType:String,
                         search:String,
                         referrer:String,
                         itemId:String,
                         googleCode:String,
                         tarea:String
                       )

case class everyClickSch(
                          timeStamp:String,
                          deviceType:String,
                          url:String,
                          search:String,
                          referrer:String,
                          itemId:String,
                          tarea:String,
                          domainSiteNo:String,
                          googleCode:String
                        )

case class EMART_OFF_SALE_LIST_SCH(
                                      SALE_DATE : String,
                                       ITEM_ID : String
                                    )

case class OFFLINE_ITEM_MPNG_SCH(
                                    OFFLINE_ITEM_ID : String,
                                    BIZCND_CD       : String,
                                    ITEM_ID         : String
                                  )

case class prodData_sch(
                         CRITN_DT         : String
                         ,DISP_SITE_NO    : String
                         ,SELL_SITE_NO    : String
                         ,ITEM_ID         : String
                         ,BRAND_ID        : String
                         ,ORD_CONT        : String
                         ,ORD_AMT         : String
                         ,RECOM_REG_CNT   : String
                         ,EVAL_SCR        : String
                         ,LIVSTK_KIND_CD  : String
                         ,LIVSTK_PAT_CD   : String
                         ,ORPLC_ID        : String
                         ,NITM_YN         : String
                         ,REGPE_ID        : String
                         ,REG_DTS         : String
                         ,MODPE_ID        : String
                         ,MOD_DTS         : String
                       )

case class TrackItemDtlGenBrtdy(visitDts:String, ip:String, siteNo:String, itemId:String, pcid:String, mbrId:String, fsid:String, timestamp:String, gen_div_cd:String, brtdy:String)
case class mbr_schema(mbr_id:String, mbr_stat_cd:String, mbr_type_cd:String, gen_div_cd:String, brtdy:String)


case class step01_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                        )
case class step02_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                        )
case class step03_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                        )
case class step04_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                        )
case class step05_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                          ,TWET_MALE_EVAL_SCR:String
                        )
case class step06_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                          ,TWET_MALE_EVAL_SCR:String
                          ,THIT_MALE_EVAL_SCR:String
                        )
case class step07_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                          ,TWET_MALE_EVAL_SCR:String
                          ,THIT_MALE_EVAL_SCR:String
                          ,FORT_MALE_EVAL_SCR:String
                        )
case class step08_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                          ,TWET_MALE_EVAL_SCR:String
                          ,THIT_MALE_EVAL_SCR:String
                          ,FORT_MALE_EVAL_SCR:String
                          ,FIFT_MALE_EVAL_SCR:String
                        )
case class step09_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                          ,TWET_MALE_EVAL_SCR:String
                          ,THIT_MALE_EVAL_SCR:String
                          ,FORT_MALE_EVAL_SCR:String
                          ,FIFT_MALE_EVAL_SCR:String
                          ,TWET_FMALE_EVAL_SCR:String
                        )
case class step10_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                          ,TWET_MALE_EVAL_SCR:String
                          ,THIT_MALE_EVAL_SCR:String
                          ,FORT_MALE_EVAL_SCR:String
                          ,FIFT_MALE_EVAL_SCR:String
                          ,TWET_FMALE_EVAL_SCR:String
                          ,THIT_FMALE_EVAL_SCR:String
                        )
case class step11_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                          ,TWET_MALE_EVAL_SCR:String
                          ,THIT_MALE_EVAL_SCR:String
                          ,FORT_MALE_EVAL_SCR:String
                          ,FIFT_MALE_EVAL_SCR:String
                          ,TWET_FMALE_EVAL_SCR:String
                          ,THIT_FMALE_EVAL_SCR:String
                          ,FORT_FMALE_EVAL_SCR:String
                        )


case class step12_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                          ,TWET_MALE_EVAL_SCR:String
                          ,THIT_MALE_EVAL_SCR:String
                          ,FORT_MALE_EVAL_SCR:String
                          ,FIFT_MALE_EVAL_SCR:String
                          ,TWET_FMALE_EVAL_SCR:String
                          ,THIT_FMALE_EVAL_SCR:String
                          ,FORT_FMALE_EVAL_SCR:String
                          ,FIFT_FMALE_EVAL_SCR:String
                        )
case class step13_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                          ,TWET_MALE_EVAL_SCR:String
                          ,THIT_MALE_EVAL_SCR:String
                          ,FORT_MALE_EVAL_SCR:String
                          ,FIFT_MALE_EVAL_SCR:String
                          ,TWET_FMALE_EVAL_SCR:String
                          ,THIT_FMALE_EVAL_SCR:String
                          ,FORT_FMALE_EVAL_SCR:String
                          ,FIFT_FMALE_EVAL_SCR:String
                          ,ALL_AGEGRP_MALE_EVAL_SCR:String
                        )
case class step14_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                          ,TWET_MALE_EVAL_SCR:String
                          ,THIT_MALE_EVAL_SCR:String
                          ,FORT_MALE_EVAL_SCR:String
                          ,FIFT_MALE_EVAL_SCR:String
                          ,TWET_FMALE_EVAL_SCR:String
                          ,THIT_FMALE_EVAL_SCR:String
                          ,FORT_FMALE_EVAL_SCR:String
                          ,FIFT_FMALE_EVAL_SCR:String
                          ,ALL_AGEGRP_MALE_EVAL_SCR:String
                          ,ALL_AGEGRP_FMALE_EVAL_SCR:String
                        )
case class step15_schema(
                          itemId:String
                          ,siteNo:String
                          ,dispSiteNo:String
                          ,brandId:String
                          ,TWET_ALL_GEN_EVAL_SCR:String
                          ,THIT_ALL_GEN_EVAL_SCR:String
                          ,FORT_ALL_GEN_EVAL_SCR:String
                          ,FIFT_ALL_GEN_EVAL_SCR:String
                          ,TWET_MALE_EVAL_SCR:String
                          ,THIT_MALE_EVAL_SCR:String
                          ,FORT_MALE_EVAL_SCR:String
                          ,FIFT_MALE_EVAL_SCR:String
                          ,TWET_FMALE_EVAL_SCR:String
                          ,THIT_FMALE_EVAL_SCR:String
                          ,FORT_FMALE_EVAL_SCR:String
                          ,FIFT_FMALE_EVAL_SCR:String
                          ,ALL_AGEGRP_MALE_EVAL_SCR:String
                          ,ALL_AGEGRP_FMALE_EVAL_SCR:String
                          ,ALL_AGEGRP_GEN_EVAL_SCR:String
                        )


object Schema {

  private val itemSchema          = "itemId mblNm splVenId brandId sellStatCd stdCtgId itemSellTypeCd itemSellTypeDtlCd dispStrtDts"
  private val stdCtgSchema        = "stdCtgId stdCtgNm priorStdCtgId stdCtgLvl stdCtgLclsId stdCtgLclsNm stdCtgMclsId stdCtgMclsNm stdCtgSclsId stdCtgSclsNm stdCtgDclsId stdCtgDclsNm itemMngPropClsId stdCtgGrpCd useYn"
  private val itemPriceSchema     = "itemId siteNo salestrNo sellPrc lwstSellPrc"
  private val mbrSchema           = "mbrId mbrLoginId genDivCd brtdy"
  private val ordItemSchema       = "orordNo ordNo ordItemDivCd ordDt dvicDivCd ordItemStatCd itemId siteNo mbrId sellPrc splPrc orordQty orordAmt stdCtgId brandId infloSiteNo splVenId regDts"
  private val trackItemDtlSchema  = "timeStamp siteNo itemId pcid mbrId"
  private val eventSchema         = "itemId"
  private val loginHistorySchema  = "mbrId pcid updateDate"
  private val cartView            = "mbrId pcid itemId visitDts cartItemSiteNo"


  def makeStruct(schema:String) : StructType = {
    StructType(
      matchSchema(schema).split(" ").map(fieldName => StructField(fieldName, StringType, true))
    )
  }

  def matchSchema(schema:String) : String = schema match {
    case "item"           => itemSchema
    case "stdCtg"         => stdCtgSchema
    case "itemPrice"      => itemPriceSchema
    case "mbr"            => mbrSchema
    case "ordItem"        => ordItemSchema
    case "event"          => eventSchema
    case "trackItemDtls"  => trackItemDtlSchema
    case "loginHistory"   => loginHistorySchema
    case "cartView"       => cartView
  }
}
