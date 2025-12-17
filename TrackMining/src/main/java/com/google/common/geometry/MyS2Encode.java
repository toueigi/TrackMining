package com.google.common.geometry;

public class MyS2Encode{
    /** The id of the cell. */
    private final long id;
    public static final int FACE_BITS = 3;
    public static final int TIME_SPAN_BITS = 16;
    public static final int POSITION_BITS = 64 - FACE_BITS - TIME_SPAN_BITS -1 ;
    public static final int MAX_LEVEL = (64 - FACE_BITS - TIME_SPAN_BITS -1) /2 ; // Valid levels: 0..MAX_LEVEL


    public MyS2Encode(long id) {
        this.id = id;
    }

    public MyS2Encode() {
        this.id = 0;
    }

    /** The 64-bit unique identifier for this cell. */
    public long id() {
        return id;
    }

    //
    /** Return the leaf cell containing the given S2LatLng. */
    public static MyS2Encode fromLatLng(Long timespan, S2LatLng ll) {
        long timeInfo = timespan;
        S2CellId s2CellId =  S2CellId.fromPoint(ll.toPoint());
        return MyS2EncodeIdFromS2(timeInfo, s2CellId);
    }

    /** Return the leaf cell containing the given S2LatLng. */
    public static MyS2Encode fromS2CellId(Long timespan, S2CellId s2CellId) {
        long timeInfo = timespan;
        return MyS2EncodeIdFromS2(timeInfo, s2CellId);
    }

    /**
     * 获取指定编码级别的自定义自定义网格编码
     * @param level 编码级别 必须小于20
     * @return 返回该编码级别的网格
     */
    public MyS2Encode parent(int level) {
        if(level > MAX_LEVEL){
            System.out.println("指定的网格级别: " + level +"已经超过最大级别"+ POSITION_BITS / 2);
            level = MAX_LEVEL;
        }
        long myS2EncodeId = id;
        //System.out.println(Long.toBinaryString(myS2EncodeId)); // 输出: 1111011
        // 获取高12位
        long faceBit = myS2EncodeId & 0xE000000000000000L; // 获取高3位的值
        //System.out.println("face位: " + Long.toBinaryString(faceBit));

        //创建Face位的0掩码，其中第64位至第FACE_BITS位之外的所有位都为1，第64位至第FACE_BITS位都为0
        long timeSpanMask = (1L << (64 - FACE_BITS )) - 1; // 创建掩码
        // &运算后，右移动POSITION_BITS +1位，以此获取时间的位数
        long timeBit = (myS2EncodeId & timeSpanMask) >>> (POSITION_BITS +1); // 获取低N位的值
        //System.out.println("timeSpan位: " + Long.toBinaryString(timeBit));

        //创建Face+Time位的0掩码，其中第64位至第时间最后一位之外的所有位都为1，前FACE_BITS+TIME_SPAN_BITS位都为0
        long positionMask = (1L << (64 - FACE_BITS - TIME_SPAN_BITS)) - 1; // 创建掩码
        //右边移动时间的位数，将时间位数剔除
        long positionBit = (myS2EncodeId & positionMask) << TIME_SPAN_BITS; // 获取低N位的值
        //System.out.println("低"+POSITION_BITS+"位: " + Long.toBinaryString(positionBit));
        //将face位+pisition位，拼出原始的S2网格编码，其中左移的位数补0
        //利用S2原始网格编码，获取他上一级别的网格编码
        S2CellId s2 =  new S2CellId( (faceBit | positionBit) ).parent(level);
        return MyS2EncodeIdFromS2(timeBit ,s2);
    }

    public String decodeFromMyS2EncodeId(){
        long myS2EncodeId = id;
        //System.out.println(Long.toBinaryString(myS2EncodeId)); // 输出: 1111011
        // 获取高12位
        long faceBit = myS2EncodeId & 0xE000000000000000L; // 获取高3位的值
        //System.out.println("face位: " + Long.toBinaryString(faceBit));

        //创建Face位的0掩码，其中第64位至第FACE_BITS位之外的所有位都为1，第64位至第FACE_BITS位都为0
        long timeSpanMask = (1L << (64 - FACE_BITS )) - 1; // 创建掩码
        // &运算后，右移动POSITION_BITS +1位，以此获取时间的位数
        long timeBit = (myS2EncodeId & timeSpanMask) >>> (POSITION_BITS +1); // 获取低N位的值
        //System.out.println("timeSpan位: " + Long.toBinaryString(timeBit));

        //创建Face+Time位的0掩码，其中第64位至第时间最后一位之外的所有位都为1，前FACE_BITS+TIME_SPAN_BITS位都为0
        long positionMask = (1L << (64 - FACE_BITS - TIME_SPAN_BITS)) - 1; // 创建掩码
        //右边移动时间的位数，将时间位数剔除
        long positionBit = (myS2EncodeId & positionMask) << TIME_SPAN_BITS; // 获取低N位的值
        //System.out.println("低"+POSITION_BITS+"位: " + Long.toBinaryString(positionBit));
        //将face位+pisition位，拼出原始的S2网格编码，其中左移的位数补0
        //利用S2原始网格编码，获取他上一级别的网格编码
        S2CellId s2 =  new S2CellId( (faceBit | positionBit) );
        return timeBit+ "|" +s2.id();
    }

    /**
     *
     * @param timeSpan  时间切片，即：（当前时间戳-当天零点零分）/时间切片（最小时间切片为25秒）
     * @param s2CellId  传统的S2网格id
     * @return 融合时间切片和S2网格的压缩编码
     */
    private static MyS2Encode MyS2EncodeIdFromS2(Long timeSpan ,S2CellId s2CellId){
        //【1】获取face值，为s2CellId的高3位，无符号右移位
        long faceBit = s2CellId.id() >>> (64 - FACE_BITS);
        //long high61AndAbove = s2CellId.id() & 0xE000000000000000L;
        long newFaceBit = faceBit << (64 - FACE_BITS);
        //【2】时间切片左移动POSITION_BITS + 1位，其中1为结束位
        long timeSpanBit = (long) timeSpan << POSITION_BITS + 1 ;
        //【3】保留高4位及以后的数据，即位置数据，然后右移TIME_SPAN_BITS的位置，用于后续存储时间信息
        long positionBit = (s2CellId.id() & 0x1FFFFFFFFFFFFFFFL) >>> TIME_SPAN_BITS ;
        //【4】三者位数进行合并,返回合并结果
        return new MyS2Encode(newFaceBit | timeSpanBit | positionBit);
    }

}
