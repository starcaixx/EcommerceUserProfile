package etl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import jdk.nashorn.internal.ir.ObjectNode;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.SparkUtils;

import java.util.List;
import java.util.stream.Collectors;

public class MemberEtl {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();

        List<MemberSex> memberSexes = memberSexEtl(session);
        List<MemberChannel> memberChannels = memberChannelEtl(session);
        List<MemberMpSub> memberMpSubs = memberMpSubEtl(session);
        MemberHeat memberHeat = memberHeatEtl(session);

        MemberVo memberVo = new MemberVo();
        memberVo.setMemberChannels(memberChannels);
        memberVo.setMemberHeat(memberHeat);
        memberVo.setMemberMpSubs(memberMpSubs);
        memberVo.setMemberSexes(memberSexes);

        System.out.println("=======");
    }


    public static List<MemberSex> memberSexEtl(SparkSession session) {
        Dataset<Row> dataset = session.sql("select sex as memberSex,count(id) as sexCount" +
                "from ecommerce.t_member group by sex");


        List<MemberSex> result = dataset.toJSON().collectAsList().stream().map(str ->{
            MemberSex memberSex = null;
            try {
                memberSex = objectMapper.readValue(str, MemberSex.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return memberSex;
        }).collect(Collectors.toList());
        return result;
    }

    public static List<MemberChannel> memberChannelEtl(SparkSession session) {
        Dataset<Row> dataset = session.sql("select member_channel as memberChannel,count(id) as channelCount" +
                "from ecommerce.t_member group by member_channel");

        List<MemberChannel> result = dataset.toJSON().collectAsList().stream().map(str ->{
            MemberChannel memberChannel = null;
            try {
                memberChannel = objectMapper.readValue(str, MemberChannel.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return memberChannel;
        }).collect(Collectors.toList());
        return result;
    }

    public static List<MemberMpSub> memberMpSubEtl(SparkSession session) {
        Dataset<Row> sub = session.sql("select count(if(mp_open_id!='null',1,null)) as subCount," +
                "count(if(mp_open_id='null'1,null)) as unSubCount from ecommerce.t_member");

        List<String> list = sub.toJSON().collectAsList();

        List<MemberMpSub> result = list.stream().map(str ->{
            MemberMpSub memberMpSub = null;
            try {
                memberMpSub = objectMapper.readValue(str, MemberMpSub.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return memberMpSub;
        }).collect(Collectors.toList());

        return result;

    }

    public static MemberHeat memberHeatEtl(SparkSession session) {
        Dataset<Row> reg_complete = session.sql("select count(if(phone='null',1,null)) as reg," +
                "count(if(phone!='null',1,null)) as complete from ecommerce.t_member");
        Dataset<Row> order_again = session.sql("select count(if(t.orderCount=1,1,null)) as order," +
                "count(if(t.orderCount>=2,1,null)) as orderAgain from (select count() as orderCount,member_id from ecommerce.t_order " +
                "group by member_id) as t");

        Dataset<Row> coupon = session.sql("select count(distinct member_id) as coupon " +
                "from ecommerce.t_coupon_member");

        Dataset<Row> heat = coupon.crossJoin(reg_complete).crossJoin(order_again);
        List<String> list = heat.toJSON().collectAsList();
        List<MemberHeat> result =  list.stream().map(str->{
            MemberHeat memberHeat = null;
            try {
                memberHeat = objectMapper.readValue(str, MemberHeat.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return memberHeat;
        }).collect(Collectors.toList());

        return result.get(0);
    }


    @Data
    static class MemberVo{
        private List<MemberSex> memberSexes;
        private List<MemberChannel> memberChannels;
        private List<MemberMpSub> memberMpSubs;
        private MemberHeat memberHeat;
    }

    @Data
    class MemberSex{
        private Integer memberSex;
        private Integer sexCount;
    }

    @Data
    class MemberChannel{
        private Integer memberChannel;
        private Integer channelCount;
    }

    @Data
    class MemberMpSub{
        private Integer subCount;
        private Integer unSubCount;
    }

    @Data
    class MemberHeat{
        private Integer reg;
        private Integer complete;
        private Integer order;
        private Integer orderAgain;
        private Integer coupon;
    }

}


