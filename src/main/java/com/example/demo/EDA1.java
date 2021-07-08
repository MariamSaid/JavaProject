package com.example.demo;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.knowm.xchart.*;
import org.knowm.xchart.BitmapEncoder.BitmapFormat;
import org.knowm.xchart.style.Styler;

public class EDA1 {

    private String path;
    private static final String DELIMITER = ",";
    private int numberOfComp = 0;
    List<String> company = new ArrayList<>();
    List<Long> count = new ArrayList<>();
    List<String[]> head = new ArrayList<>();
    Map<String, Long> f;
    static int choice;

    public EDA1(String path, int numberOfComp) {
        this.f = new HashMap<>();
        this.path = path;
        this.numberOfComp = numberOfComp;
    }

    public void getCompanyCount(int choice) {
        this.choice = choice;
        int i = 0;
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("popularAreas").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> wazzuf = sparkContext.textFile(this.path);

        List<String> h = new ArrayList<>();

        h = sparkContext.textFile(this.path).take(10);
        for (int j = 0; j < 10; j++) {
            String[] tokens = h.get(j).split(",");
            head.add(tokens);
        }
        JavaRDD<String> companyDefined;

        if (EDA1.choice == 7) {
            companyDefined = wazzuf.map(EDA1::getCompany)
                    .filter(StringUtils::isNotBlank).flatMap(com -> Arrays.asList(com
                    .toLowerCase()
                    .trim()
                    .replaceAll("\\p{Punct}", "")
                    .split(","))
                    .iterator());

        } else {
            companyDefined = wazzuf.map(EDA1::getCompany)
                    .filter(StringUtils::isNotBlank).flatMap(com -> Arrays.asList(com
                    .toLowerCase()
                    .trim()
                    .replaceAll("\\p{Punct}", "")).iterator());

        }

        Map<String, Long> companyCount = companyDefined.countByValue();
        List<Map.Entry> sortedCompany = companyCount.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toList());

        for (Map.Entry<String, Long> entry : sortedCompany) {
            if (i <= numberOfComp) {
                f.put(entry.getKey(), entry.getValue());
                company.add(entry.getKey());
                count.add(entry.getValue());
            }
            i++;
        }
        LinkedHashMap<String, Long> sortedMap = new LinkedHashMap<>();

        f.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
        f = sortedMap;
        sparkContext.close();
    }

    public void displayPiChart() {
        PieChart chart = new PieChartBuilder().width(1024).height(768)
                .title("Top 10 Companies in Number of vacancies").build();

        Iterator i = company.iterator();
        Iterator c = count.iterator();
        while (i.hasNext()) {
            chart.addSeries((String) i.next(), (Long) c.next());
        }
        try {
            if (choice == 1) {
                BitmapEncoder.saveBitmap(chart, "src//main//resources//static//companyChart.jpg", BitmapFormat.JPG);
            } else if (choice == 7) {
    BitmapEncoder.saveBitmap(chart, "src//main//resources//static//skillChart.jpg", BitmapFormat.JPG);

            }
        } catch (IOException ex) {
          System.out.println("The chart Can't be saved ");
        }
    }

    public void displayChart() {
        CategoryChart chart = new CategoryChartBuilder().width(1024).height(768).title("Number of vacancies in each location").xAxisTitle("area").yAxisTitle("count").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);
        chart.addSeries("Areas Count", company, count);

        try {
            if (choice == 0) {
                BitmapEncoder.saveBitmap(chart, "src//main//resources//static//titleChart.jpg", BitmapFormat.JPG);
            } else if (choice == 2) {
                BitmapEncoder.saveBitmap(chart, "src//main//resources//static//areaChart.jpg", BitmapFormat.JPG);
            }
        } catch (IOException ex) {
            System.out.println("The chart Can't be saved ");
        }
    }

    public static String getCompany(String observation) {
        try {
           
            return observation.split(DELIMITER)[choice];

        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
}
