package userprofile.app.support;


import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Objects;

@Configuration
public class ESConfig {
    private Logger logger = LoggerFactory.getLogger(ESConfig.class);

    private static final int ADDRESS_LENGTH = 2;
    private static final String HTTP_SCHEME = "http";

    @Value("${elasticsearch.ip}")
    String[] ipAddress;

    @Bean(name = "highLevelClient")
    public RestHighLevelClient highLevelClient(@Autowired RestClientBuilder restClientBuilder) {
        restClientBuilder.setMaxRetryTimeoutMillis(60000); // 超时1min
        return new RestHighLevelClient(restClientBuilder);
    }

    @Bean
    public RestClientBuilder restClientBuilder() {
// 从配置参数里，提取ip和port，包装成HttpHost的列表
        HttpHost[] hosts = Arrays.stream(ipAddress)
                .map(this::makeHttpHost)
                .filter(Objects::nonNull)
                .toArray(HttpHost[]::new);

        logger.info("ES hosts:{}", Arrays.toString(hosts));

        return RestClient.builder(hosts);
    }

    private HttpHost makeHttpHost(String s) {
        String[] address = s.split(":");
        if (address.length == ADDRESS_LENGTH) {
            String ip = address[0];
            int port = Integer.parseInt(address[1]);
            return new HttpHost(ip, port, HTTP_SCHEME);
        } else {
            return null;
        }
    }
}
