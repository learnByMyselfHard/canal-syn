package com.lai.canalsyn.plugin;

import com.lai.canalsyn.message.Dml;

/**
 * @ Author : lai
 * @ Date   : created in  2020/8/29 14:32
 * @ Description :
 */

public interface CanalConsumer {
    boolean support(Dml dml);
    public void insert(Dml dml);
    public void update(Dml dml);
    public void delete(Dml dml);
    default void init(){

    };
}
