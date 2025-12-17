package com.track.mytest;
import com.google.common.geometry.MyS2Encode;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2CellId;

public class S2Test {


    public static void main(String[] args) {
        // 示例经纬度
        double latitude = 37.7749;
        double longitude = 122.4194;
        // 选择一个cell级别，级别越高，cell越小
        int level = 20;

        //【1】地球经纬度转化为球面坐标的代码步骤，实现(lat,lng) --> f(x,y,z)
        // 【1.1】创建S2LatLng，经纬度转弧度计算球面坐标
        S2LatLng latLng = S2LatLng.fromDegrees(latitude, longitude);
        // 【1.2】转换为S2Point，球面的点转化为球面坐标
        S2Point point = latLng.toPoint();

        //【2】球面坐标转化为平面坐标，实现f(x,y,z) --> g(face,u,v)，
        // face是正方形的六个面，u，v对应的是六个面中的一个面上的x，y坐标。
        S2CellId cellId = S2CellId.fromPoint(point).parent(level);


        //以上两行代码等价于以下一行代码
        //S2CellId cellId = S2CellId.fromLatLng(latLng).parent(level);


        // 生成S2CellId
        //S2CellId cellId = S2CellId.fromLatLng(latLng);

        // 输出Cell ID
        S2CellId cellId2 = cellId.parent(20);
        System.out.println("S2 Cell ID: " + cellId2.id());

        S2CellId cellId3 = cellId.parent(19);
        System.out.println("S2 Cell ID: " + cellId3.id());


        MyS2Encode myS2Encode = MyS2Encode.fromS2CellId(2880L, cellId2);
        System.out.println("My EncodeS2 ID: " + myS2Encode.id());
        System.out.println("My EncodeS2 ID: " + myS2Encode.parent(19).id());

        MyS2Encode myS2Encode3 = MyS2Encode.fromS2CellId(2880L, cellId3);
        System.out.println("My EncodeS2 ID: " + myS2Encode3.id());

    }
}

// 注意：这里的类和方法（如S2LatLng, S2Point, S2CellId等）是假设存在的，
// 实际使用中需要根据你的S2库Java绑定或JNI接口来替换。

