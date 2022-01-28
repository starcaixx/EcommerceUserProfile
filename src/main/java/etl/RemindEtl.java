package etl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.SparkUtils;

import javax.xml.transform.Result;
import java.time.LocalDate;
import java.time.Month;
import java.util.List;
import java.util.stream.Collectors;

public class RemindEtl {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();

        List<FreeRemindVo> freeRemindVos = freeRemingCount(session);
        List<CouponRemindVo> couponRemindVos = couponRemindCount(session);

        System.out.println(freeRemindVos);
        System.out.println(couponRemindVos);
    }

    private static List<FreeRemindVo> freeRemingCount(SparkSession session) {
        ObjectMapper objectMapper = new ObjectMapper();
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

        LocalDate now = LocalDate.of(2020, Month.NOVEMBER, 30);
        LocalDate sevenDayBefore = now.plusDays(-7);

        String sql = "select date_format(create_time,'yyyy-MM-dd') as day,\n" +
                "count(member_id) as freeCount from ecommerce.t_coupon_member \n" +
                "where coupon_id =1 and coupon_channel=2 and create_time >='%s'\n" +
                "group by date_format(create_time,'yyyy-MM-dd')";

        sql = String.format(sql, sevenDayBefore);

        Dataset<Row> dataset = session.sql(sql);
        List<FreeRemindVo> result = dataset.toJSON().collectAsList()
                .stream().map(str -> objectMapper.convertValue(str, FreeRemindVo.class))
                .collect(Collectors.toList());

        return result;
    }

    private static List<CouponRemindVo> couponRemindCount(SparkSession session) {
        ObjectMapper objectMapper = new ObjectMapper();
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

        LocalDate now = LocalDate.of(2020, Month.NOVEMBER, 30);
        LocalDate sevenDayBefore = now.plusDays(-7);

        String sql = "select \n" +
                "date_format(create_time,'yyyy-MM-dd') as day,\n" +
                "count(member_id) as couponCount\n" +
                "from ecommerce.t_coupon_member where coupon_id!=1\n" +
                "and create_time>='%s' \n" +
                "group by date_format(create_time,'yyyy-MM-dd')";
        sql = String.format(sql, sevenDayBefore);
        Dataset<Row> dataset = session.sql(sql);
        List<CouponRemindVo> result = dataset.toJSON().collectAsList().stream()
                .map(str -> objectMapper.convertValue(str, CouponRemindVo.class))
                .collect(Collectors.toList());
        return result;
    }

    @Data
    static class FreeRemindVo {
        private String day;
        private Integer freeCount;
    }

    @Data
    static class CouponRemindVo {
        private String day;
        private Integer couponCount;
    }
}
