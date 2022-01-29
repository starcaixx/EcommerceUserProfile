package userprofile.app.support;

import lombok.Data;

@Data
public class EsQueryTag {
    private String name;    // 标签名称
    private String value;    // 查询限定的值
    private String type;    // 查询类型
}
