package util;

import java.math.BigDecimal;

/**
 * @Auther: dtygfn
 * @Date: 2019/1/19 14:26
 * @Description: 数字格式化类
 */
public class NumberUtils {

    /**
     * 格式化小数
     *
     * @param num 浮点
     * @param scale 四舍五入的位数
     * @return 格式化小数
     */
    public static double formatDouble(double num, int scale) {
        /*
        Exception in thread "main" java.lang.NumberFormatException: Infinite or NaN
	at java.math.BigDecimal.<init>(BigDecimal.java:895)
	at java.math.BigDecimal.<init>(BigDecimal.java:872)
	at com.qf.sessionanalyze.util.NumberUtils.formatDouble(NumberUtils.java:21)
	at spark.analyze.UserVisitSessionAnalyzeSpark$.calcuateAndPersistAggrStat(UserVisitSessionAnalyzeSpark.scala:382)
	at spark.analyze.UserVisitSessionAnalyzeSpark$.main(UserVisitSessionAnalyzeSpark.scala:111)
	at spark.analyze.UserVisitSessionAnalyzeSpark.main(UserVisitSessionAnalyzeSpark.scala)
         */
        if(Double.isNaN(num)){
            num = 0.0;
        }
        BigDecimal bd = new BigDecimal(num);
        return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

}
