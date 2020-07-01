package com.application.presto;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

/**
*
* @author 张睿
* @date 2020-06-30
* @param
* @return
*/


public class UdfDemo
{
    private UdfDemo(){}
    @Description("两值相除")
    @ScalarFunction(value = "divide")
    @SqlType(StandardTypes.DOUBLE)
    public static double divide(@SqlType(StandardTypes.DOUBLE) double num01,@SqlType(StandardTypes.DOUBLE) double num02){
        double result = num01/num02;
        return result;
    }
}
