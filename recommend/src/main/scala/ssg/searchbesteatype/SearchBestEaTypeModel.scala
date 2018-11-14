package ssg.searchbesteatype

/*
    Author  : Sung Ryu
    Date    : 2016-05-11
    Info    :
*/
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import ssg.util.Reader
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

class SearchBestEaTypeModelExecutor {

    private var hiveContext : HiveContext = _
    private var sc : SparkContext = _


    def init() {
        val sparkConf = new SparkConf()
          .setAppName("Recommend_SearchBestEaTypeModel")
          .set("spark.hadoop.validateOutputSpecs", "false")
          .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
          .set("spark.scheduler.mode", "FAIR")
          .set("spark.sql.tungsten.enabled", "false")
          .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
          .set("spark.mesos.coarse", "true")

        sc = new SparkContext(sparkConf)
        hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    }

    def run(){

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

        val t0 = System.nanoTime()
        var t1 = System.nanoTime()

        val conf = new Configuration()

        val hdfs = FileSystem.get(conf)

        println("=============================INFO=============================")
        println ("Last Update Date : 2016-09-21")
        println ("==============================================================")

        println("=============================INFO=============================")
        //if(args == "debug") println ("DEBUG MODE : YES") else println ("DEBUG MODE : NO")
        println ("==============================================================")


        println("=============================START=============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        val reader = new Reader(sc, hiveContext)


        val sd = new SimpleDateFormat("yyyy-MM-dd")

        val calendar = Calendar.getInstance()


        //D-1
        calendar.add(Calendar.DATE, -1)
        val d1 = sd.format(calendar.getTime)

        //D-3
        calendar.add(Calendar.DATE, -2)
        val d3 = sd.format(calendar.getTime)

        //D-4
        calendar.add(Calendar.DATE, -1)
        val d4 = sd.format(calendar.getTime)

        //D-8
        calendar.add(Calendar.DATE, -4)
        val d8 = sd.format(calendar.getTime)


        reader.everyclickInfoD1D8.registerTempTable("everyClickD1D8")

        reader.reactingEventD1D8.registerTempTable("reactingEventD1D8")

        reader.contSales.registerTempTable("contSales")

        /* ( 1-1-1 ) 최신성 지수 : 검색 빈도 증감율 : MA검색 상품상세
        - MA 검색빈도 상품상세 클릭
        - MA의 리액팅 로그의 영역코드로 판별 -> 검색기획 이훈구P가 관리
        - mApp_영역별분석코드New(검색)_20160520_v1.2.xls 참고
        - 검색결과->상품상세 영역코드 분포
          검색결과|전체상품탭|상품_클릭	             333,236
          검색결과|전체상품탭|특가딜영역_상품_클릭	   9,949
          검색결과|도서취미검색결과|상품_클릭	       256
          검색결과|테마검색탭|이슈테마레이어_상품클릭 8
        */
        val searchItemDetailClickFromMA_D1_q = SQLMaker.searchItemDetailClickFromMA_q.format("timeStamp = '"+d1+"'")
        val searchItemDetailClickFromMA_D8_q = SQLMaker.searchItemDetailClickFromMA_q.format("timeStamp = '"+d8+"'")


        /* ( 1-1-2 ) 최신성 지수 : 검색 빈도 증감율 : PC/MW검색 상품상세
        - PC/MW 검색빈도 상품상세
        - 레퍼러가 검색이고, url이 기여매장 정의셋의 상품상세인 클릭으로 필터
        - 기여매장 정의셋의 /moneymall/tsv/sales-page-set/current 상품상세 유형(상품상세 내에서의 URL은 제외) 필터링 해서 중복 제거 후 고유 URL추출
        - url like '%popupItemView.ssg%' or url like '%itemSimple.ssg%' 와 같은 퀵뷰는 신세계의 경우 etc에 쌓이기 때문에, 추가할 경우 etc작업이 필요함
        - PC검색결과 내 상품랭킹 영역 클릭 외에도, 상품추천이나 다른 컬렉션에서의 상품클릭도 모두 포함됨
        - 이유는: 몰이동,명품매장 같이 도메인이 바뀌는 경우 URL에 src_area가 없어서 어느 영역 클릭인지 판별불가
        - MW은 상품랭킹 영역 클릭에 src_area안 붙음, 그 외의 src_are붙는 케이스는 아래와 같음
        - src_area | cnt
          mssgitm_n  2,700 //검색결과 없을때 추천 아이템(정상적인 MW컬렉션)
          ssglist    835   //아래는 모두 특수한 디바이스에서 생기는 값들(비 모바일웹 레퍼러 url에서 유입됨)
          elist      290
          slist      209
          mssgitm_a  208
          sgt        99
          sdlist     45
          ervw       14
          ssgrvw     14
        */
        val searchItemDetailClickFromPC_D1_q = SQLMaker.searchItemDetailClickFromPCMW_q.format("PC","PC","timeStamp = '"+d1+"'")
        val searchItemDetailClickFromPC_D8_q = SQLMaker.searchItemDetailClickFromPCMW_q.format("PC","PC","timeStamp = '"+d8+"'")
        val searchItemDetailClickFromMW_D1_q = SQLMaker.searchItemDetailClickFromPCMW_q.format("MW","MW","timeStamp = '"+d1+"'")
        val searchItemDetailClickFromMW_D8_q = SQLMaker.searchItemDetailClickFromPCMW_q.format("MW","MW","timeStamp = '"+d8+"'")

        /* ( 1-1-4 ) 최신성 지수 : 검색 빈도 증감율 : MA/PC/MW검색 장바구니
        - MA/PC/MW 검색빈도 기여매장
        - MA 검색기여매장 판단 로직
          1. 기여매장정의셋의 검색결과 전체 에 대응하는 구글코드
          2. 아이패드 등 패드 종류의 경우 디바이스는 MA로 찍히지만, 실제 URL은 PC,MW와 같은 로직으로 판단
        - PC,MW 검색기여매장 판단 로직
          1. URL이 search.ssg 와 jsonSearch.ssg 인지로 판단

        - 하루기준 데이터
        - 검색장바구니
          MA : 386,301
          MW : 42,331
          PC : 199,040
        - 검색제외장바구니
          MA : 603,514
          MW : 106,509
          PC : 452,423
        - 전체
          MA : 989,815
          MW : 148,840
          PC : 651,463
        */
        val searchCartClickFromMA_D1_q = SQLMaker.searchCartClickFromMA_q.format("timeStamp = '"+d1+"'")
        val searchCartClickFromMA_D8_q = SQLMaker.searchCartClickFromMA_q.format("timeStamp = '"+d8+"'")
        val searchCartClickFromPC_D1_q = SQLMaker.searchCartClickFromPCMW_q.format("PC-CART","PC","timeStamp = '"+d1+"'")
        val searchCartClickFromPC_D8_q = SQLMaker.searchCartClickFromPCMW_q.format("PC-CART","PC","timeStamp = '"+d8+"'")
        val searchCartClickFromMW_D1_q = SQLMaker.searchCartClickFromPCMW_q.format("MW-CART","MW","timeStamp = '"+d1+"'")
        val searchCartClickFromMW_D8_q = SQLMaker.searchCartClickFromPCMW_q.format("MW-CART","MW","timeStamp = '"+d8+"'")

        // ( 1-1-5 ) 최신성 지수 : 검색 빈도 증감율 : UNION해서 아이템별로 cnt구하기
        val searchItemDetailClickD1FromMA = hiveContext.sql(searchItemDetailClickFromMA_D1_q)
        val searchItemDetailClickD1FromPC = hiveContext.sql(searchItemDetailClickFromPC_D1_q)
        val searchItemDetailClickD1FromMW = hiveContext.sql(searchItemDetailClickFromMW_D1_q)
        val searchCartClickD1FromMA = hiveContext.sql(searchCartClickFromMA_D1_q)
        val searchCartClickD1FromPC = hiveContext.sql(searchCartClickFromPC_D1_q)
        val searchCartClickD1FromMW = hiveContext.sql(searchCartClickFromMW_D1_q)

        searchItemDetailClickD1FromPC.unionAll(searchItemDetailClickD1FromMW).unionAll(searchCartClickD1FromPC).unionAll(searchCartClickD1FromMW).unionAll(searchItemDetailClickD1FromMA).unionAll(searchCartClickD1FromMA).registerTempTable("searchClickTempD1")

        val searchItemDetailClickD8FromMA = hiveContext.sql(searchItemDetailClickFromMA_D8_q)
        val searchItemDetailClickD8FromPC = hiveContext.sql(searchItemDetailClickFromPC_D8_q)
        val searchItemDetailClickD8FromMW = hiveContext.sql(searchItemDetailClickFromMW_D8_q)
        val searchCartClickD8FromMA = hiveContext.sql(searchCartClickFromMA_D8_q)
        val searchCartClickD8FromPC = hiveContext.sql(searchCartClickFromPC_D8_q)
        val searchCartClickD8FromMW = hiveContext.sql(searchCartClickFromMW_D8_q)

        searchItemDetailClickD8FromPC.unionAll(searchItemDetailClickD8FromMW).unionAll(searchCartClickD8FromPC).unionAll(searchCartClickD8FromMW).unionAll(searchItemDetailClickD8FromMA).unionAll(searchCartClickD8FromMA).registerTempTable("searchClickTempD8")

        hiveContext.sql(SQLMaker.sumByItem_q.format("cnt","searchClickTempD1")).registerTempTable("searchClickD1")

        hiveContext.sql(SQLMaker.sumByItem_q.format("cnt","searchClickTempD8")).registerTempTable("searchClickD8")

        // ( 1-1-6 ) 최신성 지수 : 검색 빈도 증감율 : 전체 cnt구하기
        t1 = System.nanoTime()
        println("=============================allSrchCntD1 collect() start =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        val allSrchCntD1 = hiveContext.sql(SQLMaker.totalCnt_q.format("searchClickTempD1")).select("cnt").rdd.map(r => r(0).asInstanceOf[Long]).collect()  //full 스캔탐

        t1 = System.nanoTime()
        println("=============================allSrchCntD1 collect() end =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        t1 = System.nanoTime()
        println("=============================allSrchCntD8 collect() start =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        val allSrchCntD8 = hiveContext.sql(SQLMaker.totalCnt_q.format("searchClickTempD8")).select("cnt").rdd.map(r => r(0).asInstanceOf[Long]).collect() //full 스캔탐

        t1 = System.nanoTime()
        println("=============================allSrchCntD8 collect() end =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")


        println("=================================================")
        println("allSrchCntD1    : " + allSrchCntD1(0)   )
        println("allSrchCntD8    : " + allSrchCntD8(0)   )
        println("=================================================")

        hiveContext.dropTempTable("searchClickTempD1")
        hiveContext.dropTempTable("searchClickTempD8")


        // ( 1-1-7 ) 최신성 지수 : 검색 빈도 증감율 : 증감율 계산
        val srchFrqIncrRt = hiveContext.sql(SQLMaker.srchFrqIncrRt_q.format(allSrchCntD1(0),allSrchCntD8(0)))

        srchFrqIncrRt.registerTempTable("srchFrqIncrRt")


        /* ( 1-2-1 ) 최신성 지수 : 클릭 빈도 증감율 : MA전체 상품상세(네이티브,비네이티)
        - MA의 전체 상품상세 클릭
        - 네이티브화 한 상품상세는 구글코드로 추출(현재 검색조건 제외 로직 없음->판별불가)
        - 네이티브화 아직 안한 딜아이템뷰, 명품매장 등은 tsv의 상품상세url으로 추출
        */

        // MA의 네이티브 상품상세 구글코드
        val nativeItemDetailClickFromMA_D1_q = SQLMaker.itemDetailClickFromMA_q.format("timeStamp = '"+d1+"'")
        val nativeItemDetailClickFromMA_D8_q = SQLMaker.itemDetailClickFromMA_q.format("timeStamp = '"+d8+"'")

        // MA 비네이티브 상품상세 URL
        val webItemDetailClickFromMA_D1_q = SQLMaker.itemDetailClickFromMAMWPC_q.format("MA", "MA", "timeStamp = '"+d1+"'")
        val webItemDetailClickFromMA_D8_q = SQLMaker.itemDetailClickFromMAMWPC_q.format("MA", "MA", "timeStamp = '"+d8+"'")


        /* (1-2-2 ) 최신성 지수 : 클릭 빈도 증감율 : PC/MW전체 상품상세
        - PC/MW전체 상품상세
        - 기여매장정의셋의 상품상세 url로 추출(itemDetailDescOrgView(상품상세원본보기), mItemCardBenefit(제휴카드안내), specialShopDetails (상품사이즈안내) 등 페이지 클릭 제외)
        */
        val itemDetailClickFromMW_D1_q = SQLMaker.itemDetailClickFromMAMWPC_q.format("MW", "MW", "timeStamp = '"+d1+"'")
        val itemDetailClickFromMW_D8_q = SQLMaker.itemDetailClickFromMAMWPC_q.format("MW", "MW", "timeStamp = '"+d8+"'")
        val itemDetailClickFromPC_D1_q = SQLMaker.itemDetailClickFromMAMWPC_q.format("PC", "PC", "timeStamp = '"+d1+"'")
        val itemDetailClickFromPC_D8_q = SQLMaker.itemDetailClickFromMAMWPC_q.format("PC", "PC", "timeStamp = '"+d8+"'")


        /* (1-2-4 ) 최신성 지수 : 클릭 빈도 증감율 : MA/PC/MW전체 장바구니
        - MA/PC/MW전체 장바구니
        */
        val cartClickFromAllDevice_D1_q = SQLMaker.cartClickFromAllDevice_q.format(d1)
        val cartClickFromAllDevice_D8_q = SQLMaker.cartClickFromAllDevice_q.format(d8)


        /* (1-2-5 ) 최신성 지수 : 클릭 빈도 증감율 : 아이템별 취합
        - D1 D8 클릭수량 디바이스별 테이블 생성
        */
        val nativeItemDetailClickD1FromMA = hiveContext.sql(nativeItemDetailClickFromMA_D1_q)
        val webItemDetailClickD1FromMA = hiveContext.sql(webItemDetailClickFromMA_D1_q)
        val itemDetailClickD1FromPC = hiveContext.sql(itemDetailClickFromPC_D1_q)
        val itemDetailClickD1FromMW = hiveContext.sql(itemDetailClickFromMW_D1_q)
        val cartClickD1FromAllDevice = hiveContext.sql(cartClickFromAllDevice_D1_q)

        itemDetailClickD1FromPC.unionAll(itemDetailClickD1FromMW).unionAll(cartClickD1FromAllDevice).unionAll(nativeItemDetailClickD1FromMA).unionAll(webItemDetailClickD1FromMA).registerTempTable("itemClickTempD1")

        val nativeItemDetailClickD8FromMA = hiveContext.sql(nativeItemDetailClickFromMA_D8_q)
        val webItemDetailClickD8FromMA = hiveContext.sql(webItemDetailClickFromMA_D8_q)
        val itemDetailClickD8FromPC = hiveContext.sql(itemDetailClickFromPC_D8_q)
        val itemDetailClickD8FromMW = hiveContext.sql(itemDetailClickFromMW_D8_q)
        val cartClickD8FromAllDevice = hiveContext.sql(cartClickFromAllDevice_D8_q)

        itemDetailClickD8FromPC.unionAll(itemDetailClickD8FromMW).unionAll(cartClickD8FromAllDevice).unionAll(nativeItemDetailClickD8FromMA).unionAll(webItemDetailClickD8FromMA).registerTempTable("itemClickTempD8")


        //전체 상품상세,장바구니에서 위에서 구한 검색 상품상세,장바구니를 빼줌
        hiveContext.sql(SQLMaker.sumByItemMinusSearchClick_q.format("cnt","cnt","cnt","itemClickTempD1","searchClickD1")).registerTempTable("itemClickD1")
        hiveContext.sql(SQLMaker.sumByItemMinusSearchClick_q.format("cnt","cnt","cnt","itemClickTempD8","searchClickD8")).registerTempTable("itemClickD8")

        hiveContext.dropTempTable("searchClickD1")
        hiveContext.dropTempTable("searchClickD8")

        hiveContext.dropTempTable("itemClickTempD1")
        hiveContext.dropTempTable("itemClickTempD8")

        /* (1-2-6 ) 최신성 지수 : 클릭 빈도 증감율 : 전체 카운트
        - D1 D8 전체 클릭 수량
        */
        t1 = System.nanoTime()
        println("=============================allClickCntD1 collect() start =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        val allClickCntD1 = hiveContext.sql(SQLMaker.totalCnt_q.format("itemClickD1")).select("cnt").rdd.map(r => r(0).asInstanceOf[Long]).collect()

        t1 = System.nanoTime()
        println("=============================allClickCntD1 collect() end =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        t1 = System.nanoTime()
        println("=============================allClickCntD8 collect() start =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        val allClickCntD8 = hiveContext.sql(SQLMaker.totalCnt_q.format("itemClickD8")).select("cnt").rdd.map(r => r(0).asInstanceOf[Long]).collect()

        t1 = System.nanoTime()
        println("=============================allClickCntD8 collect() end =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        println("=================================================")
        println("allClickCntD1    : " + allClickCntD1(0)   )
        println("allClickCntD8    : " + allClickCntD8(0)   )
        println("=================================================")

        /* (1-2-7 ) 최신성 지수 : 클릭 빈도 증감율 : 증감율 계산
        */
        val clickFrqIncrRt = hiveContext.sql(SQLMaker.clickFrqIncrRt_q.format(allClickCntD1(0),allClickCntD8(0)))

        clickFrqIncrRt.registerTempTable("clickFrqIncrRt")

        hiveContext.dropTempTable("itemClickD1")
        hiveContext.dropTempTable("itemClickD8")


        /* ( 1-3-1 ) 최신성 지수 :  상품 최신 등록 지수
        - 밑에 합치는 로직에서 구함
        */


        /* ( 2-1-1 ) 인기도 지수 : 검색 빈도
        - 검색 빈도 : 3일간 총 검색 수량(장바구니 제외)
        */
        val searchItemDetailClickFromMA_3Days_q = SQLMaker.searchItemDetailClickFromMA_q.format("timeStamp >= '"+d3+"'")
        val searchItemDetailClickFromPC_3Days_q = SQLMaker.searchItemDetailClickFromPCMW_q.format("PC","PC","timeStamp >= '"+d3+"'")
        val searchItemDetailClickFromMW_3Days_q = SQLMaker.searchItemDetailClickFromPCMW_q.format("MW","MW","timeStamp >= '"+d3+"'")

        val searchItemDetailClickFromMA_3Days = hiveContext.sql(searchItemDetailClickFromMA_3Days_q)
        val searchItemDetailClickFromPC_3Days = hiveContext.sql(searchItemDetailClickFromPC_3Days_q)
        val searchItemDetailClickFromMW_3Days = hiveContext.sql(searchItemDetailClickFromMW_3Days_q)

        searchItemDetailClickFromMA_3Days.unionAll(searchItemDetailClickFromPC_3Days).unionAll(searchItemDetailClickFromMW_3Days).registerTempTable("searchClickTemp3Days")

        hiveContext.sql(SQLMaker.sumByItem_q.format("totalSrchCnt","searchClickTemp3Days")).registerTempTable("searchClick3Days")

        hiveContext.dropTempTable("searchClickTemp3Days")

        /* ( 2-2-1 ) 인기도 지수 : 클릭 빈도
        - 클릭 빈도 : 3일간 총 클릭 수량, 전체 상품상세
        */
        val nativeItemDetailClickFromMA_3Days_q = SQLMaker.itemDetailClickFromMA_q.format("timeStamp >= '"+d3+"'")
        val webItemDetailClickFromMA_3Days_q = SQLMaker.itemDetailClickFromMAMWPC_q.format("MA", "MA", "timeStamp >= '"+d3+"'")
        val itemDetailClickFromMW_3Days_q = SQLMaker.itemDetailClickFromMAMWPC_q.format("MW", "MW", "timeStamp >= '"+d3+"'")
        val itemDetailClickFromPC_3Days_q = SQLMaker.itemDetailClickFromMAMWPC_q.format("PC", "PC", "timeStamp >= '"+d3+"'")

        val nativeItemDetailClickFromMA_3Days = hiveContext.sql(nativeItemDetailClickFromMA_3Days_q)
        val webItemDetailClickFromMA_3Days = hiveContext.sql(webItemDetailClickFromMA_3Days_q)
        val itemDetailClickFromMW_3Days = hiveContext.sql(itemDetailClickFromMW_3Days_q)
        val itemDetailClickFromPC_3Days = hiveContext.sql(itemDetailClickFromPC_3Days_q)

        itemDetailClickFromMW_3Days.unionAll(itemDetailClickFromPC_3Days).unionAll(nativeItemDetailClickFromMA_3Days).unionAll(webItemDetailClickFromMA_3Days).registerTempTable("itemClickTemp3Days")

        //전체 클릭 빼기 3일 검색 클릭
        hiveContext.sql(SQLMaker.sumByItemMinusSearchClick_q.format("totalSrchCnt","totalSrchCnt","totalItemClickCnt","itemClickTemp3Days","searchClick3Days")).registerTempTable("itemClick3Days")

        hiveContext.dropTempTable("itemClickTemp3Days")

        /* ( 2-3-1 ) 인기도 지수 :  주문 빈도
        - 3일간 총 주문 수량
        - 밑에 통합 구매정보 로직에서 구함
        */

        /* ( 2-4-1 ) 인기도 지수 :  장바구니 빈도
        - 3일간 총 장바구니 수량
        */
        val cartClick3Days = hiveContext.sql(SQLMaker.cartClick3Days_q.format(d3))

        cartClick3Days.registerTempTable("cartClick3Days")

        hiveContext.dropTempTable("everyClickD1D8")
        hiveContext.dropTempTable("contSales")

        /* ( 2-5-1 ) 인기도 지수 : 상품평 빈도
        - 7일간 상품평이 등록된 총 수량
        */
        reader.itemReply.registerTempTable("itemReplyTemp")

        val itemReply = hiveContext.sql(SQLMaker.itemReply_q)

        itemReply.registerTempTable("itemReply")
        hiveContext.dropTempTable("itemReplyTemp")

        /* ( 2-6-1 ) 인기도 지수 : 실주문 금액
        - 3일간 주문된 총 금액
        - 통합 구매 로직에서 구함
        */

        /* ( 3-1-1 ) 가격 지수 : 상품 판매 건단가
        - 3일간 상품 총 실주문금액 / 총 판매 수량
        - 통합 구매 로직에서 구함
        */

        /* ( 3-2-1 ) 가격 지수 : 상품 할인률
        - 3일간 상품 총 할인 금액 / 3일간 상품 총 주문 금액
        - 통합 구매 로직에서 구함
        */

        //////////////////////////////////////// 통합 구매 로직////////////////////////////////////////
        reader.ordItem2.registerTempTable("ordItem")
        val ordInfo = hiveContext.sql(SQLMaker.ordInfo_q.format(d3))

        ordInfo.registerTempTable("ordInfo")

        ////////////////////////////////////////  20170421 이마트 오프라인 구매 로직////////////////////////////////////////
        reader.getOfflineItemMpng.registerTempTable("OFFLINE_ITEM_MPNG")

        reader.getEmartOffSaleList.registerTempTable("EMART_OFF_SALE_LIST")

        hiveContext.sql(SQLMaker.getEmartOffOrdFrq).registerTempTable("emartOffOrdInfo")

        //////////////////////////////////////// 최종 합치는 로직 ////////////////////////////////////////

        // 전체 유니크 아이템아이디 추출
        hiveContext.sql(SQLMaker.getDistcintItemId.format("itemClick3Days"))
                        .unionAll(hiveContext.sql(SQLMaker.getDistcintItemId.format("searchClick3Days")))
                        .unionAll(hiveContext.sql(SQLMaker.getDistcintItemId.format("srchFrqIncrRt")))
                        .unionAll(hiveContext.sql(SQLMaker.getDistcintItemId.format("clickFrqIncrRt")))
                        .unionAll(hiveContext.sql(SQLMaker.getDistcintItemId.format("cartClick3Days")))
                        .unionAll(hiveContext.sql(SQLMaker.getDistcintItemId.format("itemReply")))
                        .unionAll(hiveContext.sql(SQLMaker.getDistcintItemId.format("ordInfo")))
                        .unionAll(hiveContext.sql(SQLMaker.getDistcintItemId.format("emartOffOrdInfo")))
                        .registerTempTable("unionItemId")

        hiveContext.sql(SQLMaker.getDistcintItemId.format("unionItemId")).registerTempTable("distinctItem")

        hiveContext.dropTempTable("unionItemId")

        // 상품 가격 테이블 로
        reader.lwstPrice.registerTempTable("itemPrc")

        // 테이블 통합
        hiveContext.sql(SQLMaker.mergeTable_q).registerTempTable("mergeTable")

        hiveContext.dropTempTable("itemClick3Days")
        hiveContext.dropTempTable("searchClick3Days")
        hiveContext.dropTempTable("srchFrqIncrRt")
        hiveContext.dropTempTable("clickFrqIncrRt")
        hiveContext.dropTempTable("cartClick3Days")
        hiveContext.dropTempTable("itemReply")
        hiveContext.dropTempTable("ordInfo")
        hiveContext.dropTempTable("emartOffOrdInfo")

        hiveContext.dropTempTable("distinctItem")

        // 상품 마스터 로드 후, 교집합 구함(딜상품 제외)
        val itemArray = reader.item.filter("itemSellTypeDtlCd != 80")

        itemArray.registerTempTable("item")

        val filterMergeTable = hiveContext.sql(SQLMaker.filterMergeTable_q)

        filterMergeTable.registerTempTable("filterMergeTable")

      // 디버그 용
//        if(false) {
//          filterMergeTable.map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s".format(
//            a(0) //CRITN_DT
//            , a(1) //ITEM_ID
//            , a(2) //SRCH_TYPE_FIRS_SCR
//            , a(3) //SRCH_TYPE_SCND_SCR
//            , a(4) //SRCH_TYPE_THRD_SCR
//            , a(5) //CLICK_ICRT
//            , a(6) //SRCH_ICRT
//            , a(7) //NEW_ITEM_REG_SCR
//            , a(8) //SRCH_CNT
//            , a(9) //CLICK_CNT
//            , a(10) //CART_ITEM_QTY
//            , a(11) //ORDD_QTY
//            , a(12) //RLORD_AMT
//            , a(13) //ORD_UPRC
//            , a(14) //ITEM_DCRT
//            , a(15) //RECOM_CONT
//          )).saveAsTextFile("hdfs://master001p27.prod.moneymall.ssgbi.com:9000/tmp/searchBestEaTypeModel.debug")
//        }

        // log스케일로 변환
        hiveContext.dropTempTable("mergeTable")
        hiveContext.dropTempTable("item")

        val logScaleTable = hiveContext.sql(SQLMaker.logScaleTable_q)

        logScaleTable.registerTempTable("logScaleTable")

        hiveContext.dropTempTable("filterMergeTable")

// 디버그 용
//
//        if(false) {
//          logScaleTable.map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s".format(
//            a(0) //CRITN_DT
//            , a(1) //ITEM_ID
//            , a(2) //SRCH_TYPE_FIRS_SCR
//            , a(3) //SRCH_TYPE_SCND_SCR
//            , a(4) //SRCH_TYPE_THRD_SCR
//            , a(5) //CLICK_ICRT
//            , a(6) //SRCH_ICRT
//            , a(7) //NEW_ITEM_REG_SCR
//            , a(8) //SRCH_CNT
//            , a(9) //CLICK_CNT
//            , a(10) //CART_ITEM_QTY
//            , a(11) //ORDD_QTY
//            , a(12) //RLORD_AMT
//            , a(13) //ORD_UPRC
//            , a(14) //ITEM_DCRT
//            , a(15) //RECOM_CONT
//            , a(16) //RECOM_CONT
//            , a(17) //RECOM_CONT
//            , a(18) //RECOM_CONT
//            , a(19) //RECOM_CONT
//            , a(20) //RECOM_CONT
//            , a(21) //RECOM_CONT
//            , a(22) //RECOM_CONT
//            , a(23) //RECOM_CONT
//            , a(24) //RECOM_CONT
//          )).saveAsTextFile("hdfs://master001p27.prod.moneymall.ssgbi.com:9000/tmp/searchBestEaTypeModel.debug")
//        }

        // 정규화에 필요한 최대,최소값 추출
        t1 = System.nanoTime()
        println("============================= normalization_var_q.head() start =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        val normal_var = hiveContext.sql(SQLMaker.normalization_var_q).head()

        t1 = System.nanoTime()
        println("============================= normalization_var_q.head() end =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        val maxClickFrqIncrRt   =  normal_var(0).asInstanceOf[Float]
        val minClickFrqIncrRt   =  normal_var(1).asInstanceOf[Float]

        val maxSrchFrqIncrRt    =  normal_var(2).asInstanceOf[Float]
        val minSrchFrqIncrRt    =  normal_var(3).asInstanceOf[Float]

        val maxItemRegIndex     =  normal_var(4).asInstanceOf[Float]
        val minItemRegIndex     =  normal_var(5).asInstanceOf[Float]

        val maxTotalSrchCnt     =  normal_var(6).asInstanceOf[Float]
        val minTotalSrchCnt     =  normal_var(7).asInstanceOf[Float]

        val maxTotalItemClickCnt=  normal_var(8).asInstanceOf[Float]
        val minTotalItemClickCnt=  normal_var(9).asInstanceOf[Float]

        val maxOrdFrq           = normal_var(10).asInstanceOf[Float]
        val minOrdFrq           = normal_var(11).asInstanceOf[Float]

        val maxTotalCartCnt     = normal_var(12).asInstanceOf[Float]
        val minTotalCartCnt     = normal_var(13).asInstanceOf[Float]

        val maxReplyCnt         = normal_var(14).asInstanceOf[Float]
        val minReplyCnt         = normal_var(15).asInstanceOf[Float]

        val maxRlordAmt         = normal_var(16).asInstanceOf[Float]
        val minRlordAmt         = normal_var(17).asInstanceOf[Float]

        val maxUPrc             = normal_var(18).asInstanceOf[Float]
        val minUPrc             = normal_var(19).asInstanceOf[Float]

        val maxItemDcRt         = normal_var(20).asInstanceOf[Float]
        val minItemDcRt         = normal_var(21).asInstanceOf[Float]

        val maxEmartOffOrdFrq   = normal_var(22).asInstanceOf[Float]
        val minEmartOffOrdFrq   = normal_var(23).asInstanceOf[Float]

        val diffClickFrqIncrRt    = maxClickFrqIncrRt     - minClickFrqIncrRt
        val diffSrchFrqIncrRt     = maxSrchFrqIncrRt      - minSrchFrqIncrRt
        val diffItemRegIndex      = maxItemRegIndex       - minItemRegIndex
        val diffTotalSrchCnt      = maxTotalSrchCnt       - minTotalSrchCnt
        val diffTotalItemClickCnt = maxTotalItemClickCnt  - minTotalItemClickCnt
        val diffOrdFrq            = maxOrdFrq             - minOrdFrq
        val diffTotalCartCnt      = maxTotalCartCnt       - minTotalCartCnt
        val diffReplyCnt          = maxReplyCnt           - minReplyCnt
        val diffRlordAmt          = maxRlordAmt           - minRlordAmt
        val diffUPrc              = maxUPrc               - minUPrc
        val diffItemDcRt          = maxItemDcRt           - minItemDcRt
        val diffEmartOffOrdFrq    = maxEmartOffOrdFrq     - minEmartOffOrdFrq

        println("=================================================")
        println("maxClickFrqIncrRt    : " + maxClickFrqIncrRt   )
        println("minClickFrqIncrRt    : " + minClickFrqIncrRt   )
        println("maxSrchFrqIncrRt     : " + maxSrchFrqIncrRt    )
        println("minSrchFrqIncrRt     : " + minSrchFrqIncrRt    )
        println("maxItemRegIndex      : " + maxItemRegIndex     )
        println("minItemRegIndex      : " + minItemRegIndex     )
        println("maxTotalSrchCnt      : " + maxTotalSrchCnt     )
        println("minTotalSrchCnt      : " + minTotalSrchCnt     )
        println("maxTotalItemClickCnt : " + maxTotalItemClickCnt)
        println("minTotalItemClickCnt : " + minTotalItemClickCnt)
        println("maxOrdFrq            : " + maxOrdFrq           )
        println("minOrdFrq            : " + minOrdFrq           )
        println("maxTotalCartCnt      : " + maxTotalCartCnt     )
        println("minTotalCartCnt      : " + minTotalCartCnt     )
        println("maxReplyCnt          : " + maxReplyCnt         )
        println("minReplyCnt          : " + minReplyCnt         )
        println("maxRlordAmt          : " + maxRlordAmt         )
        println("minRlordAmt          : " + minRlordAmt         )
        println("maxUPrc              : " + maxUPrc             )
        println("minUPrc              : " + minUPrc             )
        println("maxItemDcRt          : " + maxItemDcRt         )
        println("minItemDcRt          : " + minItemDcRt         )
        println("maxEmartOffOrdFrq    : " + maxEmartOffOrdFrq   )
        println("minEmartOffOrdFrq    : " + minEmartOffOrdFrq   )
        println("=================================================")
        println("diffClickFrqIncrRt    : " + diffClickFrqIncrRt   )
        println("diffSrchFrqIncrRt     : " + diffSrchFrqIncrRt    )
        println("diffItemRegIndex      : " + diffItemRegIndex     )
        println("diffTotalSrchCnt      : " + diffTotalSrchCnt     )
        println("diffTotalItemClickCnt : " + diffTotalItemClickCnt)
        println("diffOrdFrq            : " + diffOrdFrq           )
        println("diffTotalCartCnt      : " + diffTotalCartCnt     )
        println("diffReplyCnt          : " + diffReplyCnt         )
        println("diffRlordAmt          : " + diffRlordAmt         )
        println("diffUPrc              : " + diffUPrc             )
        println("diffItemDcRt          : " + diffItemDcRt         )
        println("diffEmartOffOrdFrq    : " + diffEmartOffOrdFrq   )
        println("=================================================")


        // 정 규 화
        val normalizationFilterMergeTable = hiveContext.sql(SQLMaker.normalization_q.format(minClickFrqIncrRt,diffClickFrqIncrRt,minSrchFrqIncrRt,diffSrchFrqIncrRt,minItemRegIndex,diffItemRegIndex,minTotalSrchCnt,diffTotalSrchCnt,minTotalItemClickCnt,diffTotalItemClickCnt,minOrdFrq,diffOrdFrq,minTotalCartCnt,diffTotalCartCnt,minReplyCnt,diffReplyCnt,minRlordAmt,diffRlordAmt,minUPrc,diffUPrc,minItemDcRt,diffItemDcRt,minEmartOffOrdFrq,diffEmartOffOrdFrq))

        normalizationFilterMergeTable.registerTempTable("normalizationFilterMergeTable")

        t1 = System.nanoTime()
        println("============================= saveAsTextFile start =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")


        // 디버그 모드일때는 추가로 전체 컬럼 다 남김
        //val debugMode = if(args == "debug") true else false

        val debugMode = false


        // 랭 킹 화
        if(!debugMode) {
          val ranking = hiveContext.sql(SQLMaker.ranking_q)

          ranking.registerTempTable("ranking")

          ranking.map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s".format(
            a(0) //CRITN_DT
            , a(1) //ITEM_ID
            , a(2) //SRCH_TYPE_FIRS_SCR
            , a(3) //SRCH_TYPE_SCND_SCR
            , a(4) //SRCH_TYPE_THRD_SCR
            , a(5) //CLICK_ICRT
            , a(6) //SRCH_ICRT
            , a(7) //NEW_ITEM_REG_SCR
            , a(8) //SRCH_CNT
            , a(9) //CLICK_CNT
            , a(10) //CART_ITEM_QTY
            , a(11) //ORDD_QTY
            , a(12) //RLORD_AMT
            , a(13) //ORD_UPRC
            , a(14) //ITEM_DCRT
            , a(15) //RECOM_CONT
            , a(16) //SRCH_TYPE_FRT_SCR
            , a(17) //STR_ORD_QTY
          )).saveAsTextFile("hdfs://master001p27.prod.moneymall.ssgbi.com:9000/moneymall/recommend/searchBestCaseModel/temp")


        } else {
          //debug모드일때
          val ranking = hiveContext.sql(SQLMaker.ranking_debug_q())

          ranking.map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s".format(
            a(0) //CRITN_DT
            , a(1) //ITEM_ID
            , a(2) //SRCH_TYPE_FIRS_SCR
            , a(3) //SRCH_TYPE_SCND_SCR
            , a(4) //SRCH_TYPE_THRD_SCR
            , a(5) //CLICK_ICRT
            , a(6) //SRCH_ICRT
            , a(7) //NEW_ITEM_REG_SCR
            , a(8) //SRCH_CNT
            , a(9) //CLICK_CNT
            , a(10) //CART_ITEM_QTY
            , a(11) //ORDD_QTY
            , a(12) //RLORD_AMT
            , a(13) //ORD_UPRC
            , a(14) //ITEM_DCRT
            , a(15) //RECOM_CONT
            , a(16) //srchFrqIncrRtNormal
            , a(17) //clickFrqIncrRtNormal
            , a(18) //itemRegIndexNormal
            , a(19) //totalCartCntNormal
            , a(20) //ordFrqNormal
            , a(21) //replyCntNormal
            , a(22) //totalSrchCntNormal
            , a(23) //totalItemClickCntNormal
            , a(24) //rlordAmtNormal
            , a(25) //uPrcNormal
            , a(26) //itemDcRtNormal
            , a(27) //RAW_CLICK_ICRT
            , a(28) //RAW_SRCH_ICRT
            , a(29) //RAW_SRCH_CNT
            , a(30) //RAW_CLICK_CNT
            , a(31) //RAW_CART_ITEM_QTY
            , a(32) //RAW_ORD_QTY
            , a(33) //RAW_RLORD_AMT
            , a(34) //RLORD_UPRC
            , a(35) //RAW_RECOM_CONT
            , a(36) //SRCH_TYPE_FRTH_SCR
            , a(37) //RAW_STR_ORD_QTY
          )).saveAsTextFile("hdfs://master001p27.prod.moneymall.ssgbi.com:9000/moneymall/recommend/searchBestCaseModel/temp")
        }

        t1 = System.nanoTime()
        println("============================= saveAsTextFile end =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        t1 = System.nanoTime()
        println("============================= FileUtil.copyMerge start =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        val c = Calendar.getInstance()

        val today = sd.format(c.getTime)

        val srcPath = "/moneymall/recommend/searchBestCaseModel/temp/"

        var dstPath = if(!debugMode) { "/moneymall/recommend/searchBestCaseModel/" + today + ".allCase/" }
                      else { "/moneymall/recommend/searchBestCaseModel/" + today + ".allCase.debug/" }

        hdfs.delete(new Path(dstPath), true)

        FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, conf, null)

        t1 = System.nanoTime()
        println("============================= FileUtil.copyMerge end =============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")


        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//        val itemRank = false;
//
//        if(itemRank) {
//
//          t1 = System.nanoTime()
//          println("============================= saveAsTextFile-2 start =============================")
//          println("Elapsed Time : " + (t1 - t0) / 1000000000 + "sec")
//          println("============================================================================")
//
//          val itemRanking = hiveContext.sql(SQLMaker.itemRanking)
//
//          itemRanking.map(a => "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s".format(
//            a(0) //CRITN_DT
//            , a(1) //ITEM_ID
//            , a(2) //SRCH_TYPE_FIRS_SCR
//            , a(3) //SRCH_TYPE_SCND_SCR
//            , a(4) //SRCH_TYPE_THRD_SCR
//            , a(5) //CLICK_ICRT
//            , a(6) //SRCH_ICRT
//            , a(7) //NEW_ITEM_REG_SCR
//            , a(8) //SRCH_CNT
//            , a(9) //CLICK_CNT
//          )).saveAsTextFile("hdfs://master001p27.prod.moneymall.ssgbi.com:9000/moneymall/recommend/searchBestCaseModel/temp")
//
//          hiveContext.dropTempTable("itemPrc")
//
//          t1 = System.nanoTime()
//          println("============================= saveAsTextFile end =============================")
//          println("Elapsed Time : " + (t1 - t0) / 1000000000 + "sec")
//          println("============================================================================")
//
//          dstPath = "/moneymall/recommend/searchBestCaseModel/" + today + ".bestrank/"
//
//          hdfs.delete(new Path(dstPath), true)
//
//          FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, conf, null)
//        }

        t1 = System.nanoTime()
        println("=============================The End=============================")
        println ("Elapsed Time : " + ( t1 - t0 )/1000000000 + "sec")
        println ("============================================================================")

        sc.stop()
    }
}

object SearchBestEaTypeModel{
    def main(args:Array[String]) {

        val executor = new SearchBestEaTypeModelExecutor
        executor.init()


          executor.run()

    }
}
