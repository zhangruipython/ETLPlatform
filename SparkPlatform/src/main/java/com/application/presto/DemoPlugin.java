package com.application.presto;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * @author 张睿
 * @create 2020-06-30 11:27
 **/
public class DemoPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return  ImmutableSet.<Class<?>>builder()
                .add(UdfDemo.class)
                .build();
    }
}

