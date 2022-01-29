package etl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.SparkUtils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.Month;
import java.util.List;
import java.util.stream.Collectors;

public class WowEtl {
    private static ObjectMapper objectMapper = new ObjectMapper();
    public static void main(String[] args) throws JsonProcessingException {
        SparkSession session = SparkUtils.initSession();

        List<RegVo> regVos = regWeekCount(session);
        List<OrderVo> orderVos = orderWeekCount(session);

        System.out.println("==============="+objectMapper.writeValueAsString(regVos));
        System.out.println("==============="+objectMapper.writeValueAsString(orderVos));
    }

    private static List<RegVo> regWeekCount(SparkSession session) {
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

        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        LocalDate lastTwoWeekFirstDay = now.plusDays(-14);

        String sql = "select\n" +
                "date_format(create_time,'yyyy-MM-dd') as day,\n" +
                "count(id) as regCount from ecommerce.t_member\n" +
                "where create_time >='%s' and create_time<'%s' \n" +
                "group by date_format(create_time,'yyyy-MM-dd')";

        sql= String.format(sql, lastTwoWeekFirstDay,now);
        Dataset<Row> regDS = session.sql(sql);
        List<String> list = regDS.toJSON().collectAsList();
        list.stream().forEach(str->System.out.println(str));
        List<RegVo> result = list.stream().map(str -> {
//            objectMapper.convertValue(str, RegVo.class)
            RegVo regVo = null;
            try {
                regVo = objectMapper.readValue(str, RegVo.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return regVo;
        })
                .collect(Collectors.toList());
        return result;
    }

    private static List<OrderVo> orderWeekCount(SparkSession session) {
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
        LocalDate lastTwoWeekFirstDay = now.plusDays(-14);

        String sql = "select \n" +
                "date_format(create_time,'yyyy-MM-dd') as day,\n" +
                "count(order_id) as orderCount from ecommerce.t_order\n" +
                "where create_time >='%s' and create_time <'%s'\n" +
                "group by date_format(create_time,'yyyy-MM-dd')";

        sql= String.format(sql, lastTwoWeekFirstDay,now);
        Dataset<Row> orderDS = session.sql(sql);
        List<OrderVo> result = orderDS.toJSON().collectAsList().stream().map(str -> objectMapper.convertValue(str, OrderVo.class))
                .collect(Collectors.toList());
        return result;
    }

    @Data
    static class RegVo {
        private String day;
        private Integer regCount;
    }

    @Data
    static class OrderVo {
        private String day;
        private Integer orderCount;
    }
}
