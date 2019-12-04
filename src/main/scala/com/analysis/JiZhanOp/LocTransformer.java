package com.analysis.JiZhanOp;

import com.common.redis.RedisConnection;

import com.common.util.JZLocationConverter.LatLng;
import com.common.util.JZLocationConverter;

import java.io.*;

public class LocTransformer {

    public static void gcj02To84JZ(String dataFile, String toFile) {
        try {

            FileInputStream fs = new FileInputStream(dataFile);
            FileOutputStream fo = new FileOutputStream(toFile);

            BufferedReader bd = new BufferedReader(new InputStreamReader(fs));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fo));

            String line = null;
            int i = 0;
            while((line = bd.readLine()) != null) {


                i++;
                String[] splits = line.split(",");
                Double lng = Double.parseDouble(splits[1]);
                Double lat = Double.parseDouble(splits[2]);
                LatLng location = new LatLng(lat,lng);
                LatLng converted = JZLocationConverter.gcj02ToWgs84(location);
                String newline = splits[0] + "," + converted.toString();
                if(i % 1000 == 0)
                    System.out.println("origin: "+line + "converted: " +newline);
                bw.write(newline);
                bw.newLine();

            }
            bw.flush();
            System.out.println(i);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static JZLocationConverter.LatLng gcj02To84(Double lng, Double lat) {
        return JZLocationConverter.gcj02ToWgs84(new JZLocationConverter.LatLng(lat,lng));
    }


    public static void main(String[] args) {
        String lng_lat = "116.43913,39.938795";
        String[] splits = lng_lat.split(",");
        System.out.println( gcj02To84(Double.parseDouble(splits[0]), Double.parseDouble(splits[1])));
    }


}
