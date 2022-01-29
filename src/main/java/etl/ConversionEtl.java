package etl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.SparkUtils;


public class ConversionEtl {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws JsonProcessingException {
        SparkSession session = SparkUtils.initSession();

        ConversionVo conversionVo = conversionBehaviorCount(session);
        System.out.println(">>>>>>>>>>"+objectMapper.writeValueAsString(conversionVo));
    }

    private static ConversionVo conversionBehaviorCount(SparkSession session) {
        Dataset<Row> orderMember = session.sql("select distinct(member_id) from ecommerce.t_order where order_status=2");

        Dataset<Row> orderAgainMember = session.sql("select\n" +
                "t.member_id as member_id\n" +
                "from (select count(order_id) as orderCount,\n" +
                "member_id from ecommerce.t_order where order_status=2 group by member_id) t \n" +
                "where t.orderCount>1");

        Dataset<Row> chargeDS = session.sql("select distinct(member_id) as member_id from ecommerce.t_coupon_member where coupon_channel =1");

        Dataset<Tuple2<Row, Row>> joinDS = chargeDS.joinWith(orderAgainMember, orderAgainMember.col("member_id").equalTo(chargeDS.col("member_id")), "inner");

        long order = orderMember.count();
        long orderAgain = orderAgainMember.count();
        long chargeCoupon = joinDS.count();

        ConversionVo conversionVo = new ConversionVo();
        conversionVo.setPresent(1000l);
        conversionVo.setClick(800l);
        conversionVo.setAddCart(600l);
        conversionVo.setOrder(order);
        conversionVo.setOrderAgain(orderAgain);
        conversionVo.setChargeCoupon(chargeCoupon);

        return conversionVo;
    }

    @Data
    static class ConversionVo{
        private Long present;
        private Long click;
        private Long addCart;
        private Long order;
        private Long orderAgain;
        private Long chargeCoupon;
    }
}
