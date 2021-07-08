/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.web.bind.annotation.GetMapping;
import smile.clustering.KMeans;
import smile.data.DataFrame;

/**
 *
 * @author top
 */
@Controller
public class WuzzufController {

    @RequestMapping("/Company")
    public String getcompany(Model model) {
        String path = "src\\main\\resources\\data\\Wuzzuf_Jobs.csv";
        EDA1 d = new EDA1(path, 10);
        d.getCompanyCount(1);
        d.displayPiChart();
        model.addAttribute("message", d.f);
        return "Company";
    }

    @RequestMapping("/Area")
    public String getArea(Model model) {
        String path = "src\\main\\resources\\data\\Wuzzuf_Jobs.csv";
        EDA1 d = new EDA1(path, 10);
        d.getCompanyCount(2);
        d.displayChart();
        model.addAttribute("message", d.f);
        return "Area";
    }

    @RequestMapping("/Title")
    public String getTitle(Model model) {
        String path = "src\\main\\resources\\data\\Wuzzuf_Jobs.csv";
        EDA1 d = new EDA1(path, 10);
        d.getCompanyCount(0);
        d.displayChart();
        model.addAttribute("message", d.f);
        return "Title";
    }

    @RequestMapping("/Skill")
    public String getSkill(Model model) {
        String path = "src\\main\\resources\\data\\Wuzzuf_Jobs.csv";
        EDA1 d = new EDA1(path, 10);
        d.getCompanyCount(7);
        d.displayPiChart();
        model.addAttribute("message", d.f);
        return "Skill";
    }

    @RequestMapping("/Summary")
    public String getSummary(Model model) {
        JobDAO obj = new JobDAO();
        DataFrame df = obj.readJobFromCSV("src/main/resources/data/Wuzzuf_Jobs.csv");
        model.addAttribute("message1", df.structure().column(0));
        model.addAttribute("message2", df.structure().column(1));
        model.addAttribute("message3", df.structure().column(2));
        model.addAttribute("message4", df.summary());
        return "Summary";
    }

    @RequestMapping("/Head")
    public String getHead(Model model) {
        String path = "src\\main\\resources\\data\\Wuzzuf_Jobs.csv";
        EDA1 d = new EDA1(path, 10);
        d.getCompanyCount(0);
        model.addAttribute("message", d.head);
        return "Head";
    }

    @RequestMapping("/YearExp")
    public String getYearExp(Model model) {
        String path = "src/main/resources/data/Wuzzuf_Jobs.csv";
        JobDAO obj = new JobDAO();
        DataFrame df = obj.readJobFromCSV(path);
        List<String> f = obj.factorizeYearExp(df);
        List<String> col = new ArrayList<>();
        HashMap<String, String> objectsList = new HashMap<>();
        obj.getJobList(df).forEach(j -> {
            col.add(j.getYearsExp());
        });

        for (int c = 0; c < f.size(); c++) {
            objectsList.put(f.get(c), col.get(c));
        }
        model.addAttribute("objectsList", objectsList);
        return "YearExp";
    }

    @RequestMapping("/KMean")
    public String applyKMean(Model model) {
        String path = "src/main/resources/data/Wuzzuf_Jobs.csv";
        KMean modelKMean = new KMean();
        KMeans k = modelKMean.train(new JobDAO().readJobFromCSV(path));
        String[] arrOfStr = k.toString().split("Cluster", 5);

        for (int c = 1; c < arrOfStr.length; c++) {
            arrOfStr[c] = "Cluster".concat(arrOfStr[c]);
        }

        model.addAttribute("model", arrOfStr);
        return "KMean";
    }

}
