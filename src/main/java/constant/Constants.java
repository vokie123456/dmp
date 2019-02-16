package constant;

/**
 * 定义项目中用到的常量
 */
public interface Constants {
    // 进行判断的参数
    String REQUESTMODE = "requestmode";
    String PROCESSNODE = "processnode";
    String ISEFFECTIVE = "iseffective";
    String ISBILLING = "isbilling";
    String ISBID = "isbid";
    String ISWIN = "iswin";
    String ADORDEERID = "adorderid";
    String WINPRICE = "winprice";
    String ADPAYMENT = "adpayment";

    // 统计的结果字段
    String APPNAME = "appname";
    String PROVINCENAME = "provincename";
    String CITYNAME = "cityname";
    String ORIGINAL_REQUEST_COUNT = "original_request_count";
    String EFFECTIVE_REQUEST_COUNT = "effective_request_count";
    String AD_REQUEST_COUNT = "ad_request_count";
    String JOIN_BIDING_COUNT = "join_biding_count";
    String BIDING_WIN_COUNT = "biding_win_count";
    String BIDING_WIN_RATE = "biding_win_rate";
    String SHOW_COUNT = "show_count";
    String CLICK_COUNT = "click_count";
    String CLICK_RATE = "click_rate";
    String DSPWINPRICE = "DSPwinprice";
    String DSPADPAYMENT = "DSPadpayment";

    String zkQuorm = "hbase.zookeeper.quorum";
    // 设置zooKeeper集群地址，
    String zkQuormlist = "mini4,mini5,mini6";
    String zkQuormProt = "hbase.zookeeper.property.clientPort";
    String clientPort = "2181";




}
