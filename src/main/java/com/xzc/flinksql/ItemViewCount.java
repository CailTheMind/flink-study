package com.xzc.flinksql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author xzc
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemViewCount implements Serializable {

    private Long itemId;

    private Long windowEnd;

    private Long count;
}
