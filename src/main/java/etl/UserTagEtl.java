package etl;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import utils.SparkUtils;

import java.io.Serializable;
import java.util.List;

public class UserTagEtl {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();
        etl(session);
    }

    private static void etl(SparkSession session) {
        Dataset<Row> memberDS = session.sql("select \n" +
                "id as memberId,phone,sex,member_channel as channel,mp_open_id as subOpenId,\n" +
                "address_default_id as address,\n" +
                "date_format(create_time,'yyyy-MM-dd') as regTime from ecommerce.t_member");

        Dataset<Row> orderBehaviorDS = session.sql("select \n" +
                "o.member_id as memberId,\n" +
                "count(o.order_id) as orderCount,\n" +
                "date_format(max(o.create_time),'yyyy-MM-dd') as orderTime,\n" +
                "sum(o.pay_price) as orderMoney,\n" +
                "collect_list(distinct oc.commodity_id) as favGoods\n" +
                "from ecommerce.t_order as o left join \n" +
                "ecommerce.t_order_commodity as oc \n" +
                "on o.order_id = oc.order_id \n" +
                "group by o.member_id\n");

        Dataset<Row> freeCouponDS = session.sql("select \n" +
                "member_id as memberId,date_format(create_time,'yyyy-MM-dd') as freeCouponTime\n" +
                "from ecommerce.t_coupon_member where coupon_id = 1");

        Dataset<Row> couponTimesDS = session.sql("select member_id as memberId,\n" +
                "collect_list(date_format(create_time,'yyyy-MM-dd')) as couponTimes \n" +
                "from ecommerce.t_coupon_member where coupon_id!=1 \n" +
                "group by member_id");

        Dataset<Row> chargeMoneyDS = session.sql("select \n" +
                "cm.member_id as memberId,\n" +
                "sum(c.coupon_price/2) as chargeMoney\n" +
                "from ecommerce.t_coupon_member as cm left join \n" +
                "ecommerce.t_coupon as c \n" +
                "on cm.coupon_id=c.id \n" +
                "where cm.coupon_channel!=1 \n" +
                "group by cm.member_id");

        Dataset<Row> overTimeDS = session.sql("select \n" +
                "(to_unix_timestamp(max(arrive_time))-to_unix_timestamp(max(pick_time))) as overTime,\n" +
                "member_id as memberId\n" +
                "from ecommerce.t_delivery \n" +
                "group by member_id");

        Dataset<Row> feedbackDS = session.sql("select \n" +
                "fb.feedback_type as feedback,\n" +
                "fb.member_id as memberId\n" +
                "from ecommerce.t_feedback as fb right join \n" +
                "(select max(id) as fid,member_id as memberId\n" +
                "from ecommerce.t_feedback group by member_id) t \n" +
                "on fb.id = t.fid\n");

        memberDS.registerTempTable("memberBase");
        orderBehaviorDS.registerTempTable("orderBehavior");
        freeCouponDS.registerTempTable("freeCoupon");
        couponTimesDS.registerTempTable("couponTimes");
        chargeMoneyDS.registerTempTable("chargeMoney");
        overTimeDS.registerTempTable("overTime");
        feedbackDS.registerTempTable("feedback");

        Dataset<Row> result = session.sql("select\n" +
                "m.*," +
                "o.orderCount,o.orderTime,o.orderMoney,o.favGoods,\n" +
                "fb.freeCouponTime,ct.coupontimes,cm.chargeMoney,ot.overTime,f.feedback\n" +
                "from memberBase as m \n" +
                "left join orderBehavior as o on m.memberId = o.memberId\n" +
                "left join freeCoupon fb on m.memberId = fb.memberId\n" +
                "left join couponTimes as ct on m.memberId = ct.memberId\n" +
                "left join chargeMoney as cm on m.memberId = cm.memberId\n" +
                "left join overTime as ot on m.memberId=ot.memberId " +
                "left join feedback as f on m.memberId=f.memberId");


        JavaEsSparkSQL.saveToEs(result,"/uesrtag/_doc");
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
