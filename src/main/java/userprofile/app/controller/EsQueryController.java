package userprofile.app.controller;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import etl.UserTagEtl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import scala.util.parsing.json.JSONArray;
import scala.util.parsing.json.JSONObject;
import userprofile.app.service.EsQueryService;
import userprofile.app.support.EsQueryTag;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

@Controller
public class EsQueryController {

    @Autowired
    EsQueryService service;

    // 需要提交表单，POST请求
    @RequestMapping("/gen")
    public void genAndDown(HttpServletResponse response, @RequestBody String data) throws IOException {
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

        JsonNode jsonNode = objectMapper.readValue(data, JsonNode.class);
        List<EsQueryTag> list = objectMapper.readValue(jsonNode.get("selectedTags").asText(), new TypeReference<List<EsQueryTag>>(){});

// 调用服务，按照查询限制提取出对应的用户信息
        List<UserTagEtl.MemberTag> tags = service.buildQuery(list);

// 自定义方法，将所有用户信息拼在一起写入文件
        String content = toContent(tags);
        String fileName = "member.txt";

        response.setContentType("application/octet-stream");
        try {
            response.setHeader("Content-Disposition", "attachment; filename=" + URLEncoder.encode(fileName, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        try {
            ServletOutputStream sos = response.getOutputStream();
            BufferedOutputStream bos = new BufferedOutputStream(sos);
            bos.write(content.getBytes("UTF-8"));
            bos.flush();    // 直接将缓冲池中数据刷出，默认是要填满才发
            bos.close();
            sos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String toContent(List<UserTagEtl.MemberTag> tags) {
        StringBuilder sb = new StringBuilder();
        for (UserTagEtl.MemberTag tag : tags) {
            sb.append("[" + tag.getMemberId() + "," + tag.getPhone() + "]\r\n");
        }
        return sb.toString();
    }
}
