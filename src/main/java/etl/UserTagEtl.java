package etl;

import lombok.Data;
import org.apache.spark.sql.SparkSession;
import utils.SparkUtils;

import java.io.Serializable;
import java.util.List;

public class UserTagEtl {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();
        etl(session);
    }

    private static void etl(SparkSession session) {

    }


    // 定义用户标签的VO
    @Data
    public static class MemberTag implements Serializable {
        // 用户基本信息
        private String memberId;
        private String phone;
        private String sex;
        private String channel;
        private String subOpenId;
        private String address;
        private String regTime;

        // 用户业务行为特征
        private Long orderCount;
        private String orderTime;
        private Double orderMoney;
        private List<String> favGoods;

        // 用户购买能力
        private String freeCouponTime;   // 首单免费时间
        private List<String> couponTimes;    // 多次购买时间
        private Double chargeMoney;    // 购买花费金额

        // 用户反馈行为特征
        private Integer overTime;
        private Integer feedBack;

    }
}
