package com.application.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.CorruptedFrameException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author 张睿
 * @create 2020-06-04 15:21
 * rocksdb中value数据解码
 **/
public class ValueDecode {
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    public byte[] decodeValue(byte[] decodedValue){
        Height height = newHeightFromBytes(decodedValue);
        // length=5 [1,2,3,4,5]
        int n = height.getN3();
        byte[] value = new byte[decodedValue.length-n];
        System.arraycopy(decodedValue,n,value,0,decodedValue.length-n);
        return value;
    }
    /** protobuf 中VarInt32解码操作**/
    public int readRawVarInt32(ByteBuf byteBuf){
        //不可读
        if (!byteBuf.isReadable()) {
            return 0;
        }
        byteBuf.markReaderIndex();
        //读取一个字节
        byte tmp = byteBuf.readByte();
        if (tmp >= 0) {
            //如果大于 0 ，说明只有一个字节（如果大于一个字节，高位为1，值为负）
            return tmp;
        } else {
            //取temp低7位
            int result = tmp & 127;

            if (!byteBuf.isReadable()) {
                //如果读取一个字节后，不可再读（读指针大于等于写指针，源码注释有点问题），说明这个值只有一个字节，最高位为1，值错误返回 0。
                byteBuf.resetReaderIndex();
                return 0;
            }
            if ((tmp = byteBuf.readByte()) >= 0) {
                //取第二个字节，如果第二个字节大于0，则左移7位（只有7位表示数字，不够一个字节则高位补0），
                result |= tmp << 7;
            } else {
                //第二个字节取低7位，左移7位，与result 取或
                result |= (tmp & 127) << 7;
                if (!byteBuf.isReadable()) {
                    //值错误
                    byteBuf.resetReaderIndex();
                    return 0;
                }
                if ((tmp = byteBuf.readByte()) >= 0) {
                    //取第三个字节，和上面一样
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 127) << 14;
                    if (!byteBuf.isReadable()) {
                        byteBuf.resetReaderIndex();
                        return 0;
                    }
                    if ((tmp = byteBuf.readByte()) >= 0) {
                        //取第四个字节，和上面一样
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 127) << 21;
                        if (!byteBuf.isReadable()) {
                            byteBuf.resetReaderIndex();
                            return 0;
                        }
                        //取第五个字节
                        result |= (tmp = byteBuf.readByte()) << 28;
                        if (tmp < 0) {
                            throw new CorruptedFrameException("malformed varint.");
                        }
                    }
                }
            }
            return result;
        }
    }
    public Height newHeightFromBytes(byte[] var01){
        EncodeData encodeData = doEncode(var01);
        long blockNum = encodeData.getDecodeByte();
        int n1 = encodeData.getNumBytes();

        byte[] var02 = new byte[var01.length-n1];
        System.arraycopy(var01,n1,var02,0,var01.length-n1);
        EncodeData encodeData1 = doEncode(var02);
        long txNum =encodeData1.getDecodeByte();
        int n2 = encodeData1.getNumBytes();
        Height height = new Height();
        height.setBlockNum(blockNum);
        height.setTxNum(txNum);
        height.setN3(n1+n2);
        return height;
    }
    /** 补码操作**/
    public EncodeData doEncode(byte[] value){
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
        try {

            byteBuf.writeBytes(value);
            int a = readRawVarInt32(byteBuf);
            int numBytesConsumed = a+1;
            byte[] decodedBytes = new byte[8];
            System.arraycopy(value,1,decodedBytes,8-a,a);
            // 将byte[]转为long类型
            ByteBuffer buffer1 = ByteBuffer.wrap(decodedBytes);
            long blockNum = buffer1.getLong();
            EncodeData encodeData = new EncodeData();
            encodeData.setDecodeByte(blockNum);
            encodeData.setNumBytes(numBytesConsumed);
            return encodeData;
        }finally {
            byteBuf.release();
        }
    }

    private static class EncodeData{
        private long decodeByte;
        private int numBytes;

        public long getDecodeByte() {
            return decodeByte;
        }

        public void setDecodeByte(long decodeByte) {
            this.decodeByte = decodeByte;
        }

        public int getNumBytes() {
            return numBytes;
        }

        public void setNumBytes(int numBytes) {
            this.numBytes = numBytes;
        }
    }
    private static class Height{
        long blockNum;
        long txNum;
        int n3;

        public long getBlockNum() {
            return blockNum;
        }

        public long getTxNum() {
            return txNum;
        }

        public int getN3() {
            return n3;
        }

        public void setN3(int n3) {
            this.n3 = n3;
        }

        public void setBlockNum(long blockNum) {
            this.blockNum = blockNum;
        }

        public void setTxNum(long txNum) {
            this.txNum = txNum;
        }
    }
}

