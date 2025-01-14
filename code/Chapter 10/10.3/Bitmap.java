package com.flink.data.udf;
// 合并bitmap的逻辑
@Function(value = "merge_bitmap")
public class BitmapBase extends ScalarFunction {
    // ...
    // i为本次聚合生成的bitmap，o为从HBase表获取上个批次的bitmap
    public String eval(String i, String o){
        // i不为空o为空时，返回i
        if(!StringUtils.isBlank(i) && StringUtils.isBlank(o)){
            return i;
        }

        // i为空o不为空时，返回o
        if(StringUtils.isBlank(i) && !StringUtils.isBlank(o)){
            return o;
        }

        // i，o均为空时，返回空
        if(StringUtils.isBlank(i) && StringUtils.isBlank(o)){
            return "";
        }

        // i，o均不为空时，返回并集
        try{
            Roaring64Bitmap leftBitmap = deserializeToBitmap(i);
            Roaring64Bitmap rightBitmap = deserializeToBitmap(o);
            leftBitmap.or(rightBitmap);
            return serializeToString(leftBitmap);
        }catch (Exception e){
            // 合并失败时，返回o
            return o;
        }
    }
}
