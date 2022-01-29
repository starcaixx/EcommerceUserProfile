package userprofile.app.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import etl.UserTagEtl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;
import userprofile.app.support.EsQueryTag;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Service
public class EsQueryService {
    @Resource(name = "highLevelClient")
    RestHighLevelClient highLevelClient;

    public List<UserTagEtl.MemberTag> buildQuery(List<EsQueryTag> tags) {
        SearchRequest request = new SearchRequest();

        request.indices("usertag");
        request.types("_doc");

        SearchSourceBuilder builder = new SearchSourceBuilder();
        request.source(builder);
        String[] includes = {"memberId", "phone"};
        builder.fetchSource(includes, null);
        builder.from(0);    // 查询结果，从0开始
        builder.size(1000);   // 大小为1000，相当于 limit 1000

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        builder.query(boolQueryBuilder);
        List<QueryBuilder> should = boolQueryBuilder.should();
        List<QueryBuilder> mustNot = boolQueryBuilder.mustNot();
        List<QueryBuilder> must = boolQueryBuilder.must();

        // 遍历查询条件参数，判断是哪一类
        for (EsQueryTag tag : tags) {
            String name = tag.getName();
            String value = tag.getValue();
            String type = tag.getType();

            if (type.equals("match")) {
                should.add(QueryBuilders.matchQuery(name, value));
            }
            if (type.equals("notMatch")) {
                mustNot.add(QueryBuilders.matchQuery(name, value));
            }
            if (type.equals("rangeBoth")) {
                String[] split = value.split("-");
                String v1 = split[0];
                String v2 = split[1];
                should.add(QueryBuilders.rangeQuery(name).lte(v2).gte(v1));
            }
            if (type.equals("rangeGte")) {
                should.add(QueryBuilders.rangeQuery(name).gte(value));
            }
            if (type.equals("rangeLte")) {
                should.add(QueryBuilders.rangeQuery(name).lte(value));
            }
            if (type.equals("exists")) {
                should.add(QueryBuilders.existsQuery(name));
            }
        }

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

        RequestOptions options = RequestOptions.DEFAULT;
        List<UserTagEtl.MemberTag> memberTags = new ArrayList<>();
        try {
            SearchResponse search = highLevelClient.search(request, options);
            SearchHits hits = search.getHits();
            Iterator<SearchHit> iterator = hits.iterator();
            while (iterator.hasNext()) {
                SearchHit hit = iterator.next();
                String sourceAsString = hit.getSourceAsString();

                UserTagEtl.MemberTag memberTag = objectMapper.readValue(sourceAsString, UserTagEtl.MemberTag.class);
                memberTags.add(memberTag);
            }
            return memberTags;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
