package com.xzc.apitest;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author xzc
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {

    private String id;

    private Long timestamp;

    private Double temperature;


}
