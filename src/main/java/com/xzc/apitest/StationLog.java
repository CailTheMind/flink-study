package com.xzc.apitest;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StationLog implements Serializable {

    /**
     * 基站ID
     */
    private String stationID;
    /**
     * 呼叫放
     */
    private String from;
    /**
     * 被叫方
     */
    private String to;
    /**
     * 通话的持续时间
     */
    private long duration;
    /**
     * 通话的呼叫时间
     */
    private long callTime;

}
