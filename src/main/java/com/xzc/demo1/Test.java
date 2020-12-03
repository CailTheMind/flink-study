package com.xzc.demo1;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.xzc.flinksql.DataInfo;

public class Test {
    public static void main(String[] args) {
        String value = "{\"app_id\":\"\",\"send_time\":1605071258239,\"serial_number\":\"1000031aa778d231c6ac9bbdf9aba917147cea21605071258231\",\"data\":{\"user_guid\":\"121000047\",\"user_type\":\"4\",\"user_company_id\":\"\",\"user_org_id\":\"100003\",\"user_school_id\":\"1000020\",\"paper_id\":\"1588\",\"paper_period_id\":\"12\",\"paper_subject_id\":\"1215\",\"upload_time\":\"1605071257\"}}";
        DataInfo<TestQuestionInputDTO> result = new Gson().fromJson(value, new TypeToken<DataInfo<TestQuestionInputDTO>>() {
        }.getType());
        System.out.println("result = " + result);
    }
}
