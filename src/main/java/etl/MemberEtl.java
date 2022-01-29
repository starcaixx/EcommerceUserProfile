package etl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.SparkUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MemberEtl {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws JsonProcessingException {
        SparkSession session = SparkUtils.initSession();

        // 写sql查询数据
        List<MemberSex> memberSexes = memberSexEtl(session);
        List<MemberChannel> memberChannels = memberChannelEtl(session);
        List<MemberMpSub> memberMpSubs = memberMpSubEtl(session);
        MemberHeat memberHeat = memberHeatEtl(session);

        // 拼成需要的结果
        MemberVo memberVo = new MemberVo();
        memberVo.setMemberSexes(memberSexes);
        memberVo.setMemberMpSubs(memberMpSubs);
        memberVo.setMemberChannels(memberChannels);
        memberVo.setMemberHeat(memberHeat);
        System.out.println("====="+objectMapper.writeValueAsString(memberVo));
    }

    public static List<MemberSex> memberSexEtl(SparkSession session) {
        Dataset<Row> dataset = session.sql("select sex as memberSex,count(id) as sexCount" +
                " from ecommerce.t_member group by sex");

        List<MemberSex> result = dataset.toJSON().collectAsList().stream().map(str ->{
            MemberSex memberSex = null;
            try {
                memberSex = objectMapper.readValue(str, MemberSex.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return memberSex;
        }).collect(Collectors.toList());
        return result;
    }

    public static List<MemberChannel> memberChannelEtl(SparkSession session) {
        Dataset<Row> dataset = session.sql("select member_channel as memberChannel,count(id) as channelCount" +
                " from ecommerce.t_member group by member_channel");

        List<MemberChannel> result = dataset.toJSON().collectAsList().stream().map(str ->{
            MemberChannel memberChannel = null;
            try {
                memberChannel = objectMapper.readValue(str, MemberChannel.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return memberChannel;
        }).collect(Collectors.toList());
        return result;
    }

    public static List<MemberMpSub> memberMpSubEtl(SparkSession session) {
        Dataset<Row> sub = session.sql("select count(if(mp_open_id!='null',1,null)) as subCount," +
                "count(if(mp_open_id='null',1,null)) as unSubCount from ecommerce.t_member");

        List<String> list = sub.toJSON().collectAsList();

        List<MemberMpSub> result = list.stream().map(str ->{
            MemberMpSub memberMpSub = null;
            try {
                memberMpSub = objectMapper.readValue(str, MemberMpSub.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } catch (IOException e) {
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
            } catch (IOException e) {
                e.printStackTrace();
            }
            return memberHeat;
        }).collect(Collectors.toList());

        return result.get(0);
    }

    // 想要展示饼图的数据信息
    @Data
    static class MemberVo{
        private List<MemberSex> memberSexes;    // 性别统计信息
        private List<MemberChannel> memberChannels;  // 渠道来源统计信息
        private List<MemberMpSub> memberMpSubs;  // 用户是否关注媒体平台
        private MemberHeat memberHeat;   // 用户热度统计
    }
    // 分别定义每个元素类
    @Data
    static class MemberSex {
        private Integer memberSex;
        private Integer sexCount;
    }
    @Data
    static class MemberChannel {
        private Integer memberChannel;
        private Integer channelCount;
    }
    @Data
    static class MemberMpSub {
        private Integer subCount;
        private Integer unSubCount;
    }
    @Data
    static class MemberHeat {
        private Integer reg;    // 只注册，未填写手机号
        private Integer complete;    // 完善了信息，填了手机号
        private Integer order;    // 下过订单
        private Integer orderAgain;    // 多次下单，复购
        private Integer coupon;    // 购买过优惠券，储值
    }

}


