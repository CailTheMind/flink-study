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
public class UserBehavior implements Serializable {
    /**
     * // 用户ID
     */
    private long userId;

    /**
     * // 商品ID
     */
    private long itemId;

    /**
     * // 商品类目ID
     */
    private int categoryId;

    /**
     * // 用户行为, 包括("pv", "buy", "cart", "fav")
     */
    private String behavior;
    /**
     * // 行为发生的时间戳，单位秒
     */
    private long timestamp;
}
