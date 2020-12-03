package com.xzc.flinksql;

import lombok.Data;

import java.io.Serializable;

/**
 * @author xzc
 */
@Data
public class DataInfo<T> implements Serializable {

    private static final long serialVersionUID = 780973529733148978L;

    private T data;

    private String app_id;

    private Long send_time;

    private String serial_number;


}
