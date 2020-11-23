package com.xzc.flinksql;

import lombok.Data;

import java.io.Serializable;

/**
 * 资源上传指标
 *
 * @author xzc
 */
@Data
public class GroupPaperInputDTO implements Serializable {
    private static final long serialVersionUID = -5153419796328985022L;


    /**
     * 用户ID
     */
    private String user_guid;

    /**
     * 用户身份ID
     */
    private String user_type;

    /**
     * 用户所属公司ID
     */
    private String user_company_id;

    /**
     * 用户所属机构ID
     */
    private String user_org_id;

    /**
     * 用户所属学校ID
     */
    private String user_school_id;

    /**
     * 录入时间
     */
    private String upload_time;

    /**
     * 资源ID
     */
    private String paper_id;

    /**
     * 资源适用学段ID
     */
    private String paper_period_id;

    /**
     * 资源适用学科ID
     */
    private String paper_subject_id;
}
