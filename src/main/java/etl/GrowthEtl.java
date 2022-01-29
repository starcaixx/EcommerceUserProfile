package etl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import utils.SparkUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class GrowthEtl {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws JsonProcessingException {
        SparkSession session = SparkUtils.initSession();

        List<GrowthLineVo> growthLineVos = growthEtl(session);
        System.out.println(">>>>>>>>>>>>>"+objectMapper.writeValueAsString(growthLineVos));
    }

    private static List<GrowthLineVo> growthEtl(SparkSession session) {
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);

        LocalDate sevenDateAgo = now.plusDays(-7);

        objectMapper.registerModule(new ParanamerModule());

        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);
        objectMapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        String memberSql = "select\n" +
                "date_format(create_time,'yyyy-MM-dd') as day,\n" +
                "count(id) as regCount,\n" +
                "max(id) as memberCount\n" +
                "from ecommerce.t_member where create_time >= '%s' \n" +
                "group by date_format(create_time,'yyyy-MM-dd')\n" +
                "order by day";

        memberSql = String.format(memberSql,sevenDateAgo);

        String orderSql = "select date_format(create_time,'yyyy-MM-dd') as day,\n" +
                "max(order_id) orderCount,\n" +
                "sum(origin_price) gmv \n" +
                "from ecommerce.t_order where create_time >='%s'\n" +
                "group by date_format(create_time,'yyyy-MM-dd')\n" +
                "order by day";

        orderSql = String.format(orderSql, sevenDateAgo);
        Dataset<Row> orderDS = session.sql(orderSql);

        Dataset<Row> memberDS = session.sql(memberSql);

        Dataset<Tuple2<Row, Row>> tuple2DS = memberDS.joinWith(orderDS, memberDS.col("day").equalTo(orderDS.col("day")), "inner");

        List<Tuple2<Row, Row>> tuple2s = tuple2DS.collectAsList();

        ArrayList<GrowthLineVo> vos = new ArrayList<>();
        for (Tuple2<Row, Row> tuple2 : tuple2s) {
            Row member = tuple2._1;
            Row order = tuple2._2;

            ObjectNode objectNode = objectMapper.createObjectNode();
            for (String field : member.schema().fieldNames()) {
                Object as = member.getAs(field);
                objectNode.put(field,as==null?null:as.toString());
            }

            for (String field : order.schema().fieldNames()) {
                Object as = order.getAs(field);
                objectNode.put(field,as==null?null:as.toString());
            }

            GrowthLineVo growthLineVo = objectMapper.convertValue(objectNode, GrowthLineVo.class);
            vos.add(growthLineVo);
        }

        String preGmvSql = "select sum(origin_price) as totalGmv from ecommerce.t_order where create_time <'%s'";
        preGmvSql = String.format(preGmvSql, sevenDateAgo);
        double previousGmv = session.sql(preGmvSql).collectAsList().get(0).getDouble(0);

        BigDecimal preGmv = BigDecimal.valueOf(previousGmv);

        ArrayList<BigDecimal> totalGmvList = new ArrayList<>();
        /*for (int i = 0; i < vos.size(); i++) {
            GrowthLineVo growthLineVo = vos.get(i);
            BigDecimal gmv = growthLineVo.getGmv();
            BigDecimal temp = gmv.add(preGmv);

            for (int j = 0; j <i; j++) {
                GrowthLineVo prev = vos.get(j);

                temp=temp.add(prev.getGmv());
            }
            totalGmvList.add(temp);
        }

        for (int i = 0; i < totalGmvList.size(); i++) {
            GrowthLineVo lineVo = vos.get(i);
            lineVo.setGmv(totalGmvList.get(i));
        }*/
        return vos;
    }

    @Data
    static class GrowthLineVo{
//        每天新增注册数、总用户数、总订单数、总流水GMV
        private String day;
        private Integer regCount;
        private Integer memberCount;
        private Integer orderCount;
        private BigDecimal gmv;
    }
}
