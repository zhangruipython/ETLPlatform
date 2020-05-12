package com.application.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author 张睿
 * @create 2020-05-06 9:07
 **/
public class RegressionModelTrain {
    public static void main(String[] args) throws IOException {
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        long time01 = System.currentTimeMillis();
        SparkSession sparkSession = SparkSession.builder().appName("MlDemo").config(new SparkConf().setMaster("local[*]")).getOrCreate();
        // 读取csv中数据
        Dataset<Row> data = sparkSession.read().format("com.databricks.spark.csv").option("header","true")
                .option("inferSchema", "true").load("SCT___a___v4_prepared.csv").cache();
        // 区分字符类型变量和数值类型变量
        String str = "StringType";
        List<String> columns = new ArrayList<String>();
        data.schema().foreach(s->{
            // 判断列属性
            if(str.equals(s.dataType().toString())){
                columns.add(s.name());
            }
            return columns;
        });

        // 字符类型和总类型计算差集为数值类型变量，分割出数值类型dataframe
        List<String> numberColumns = Arrays.stream(data.columns()).filter(item->!columns.contains(item)).collect(Collectors.toList());
        Column[] numberCols = numberColumns.parallelStream().map(functions::col).toArray(Column[]::new);
        Dataset<Row> numberDf = data.select(numberCols);
        Column[] cols = columns.stream().map(functions::col).toArray(Column[]::new);
        Dataset<Row> strDf = data.select(cols);

        // 将字符类型变量转为数值类型，并添加至dataframe
        List<String> indexColumns = new ArrayList<>();
        List<String> vocColumns = new ArrayList<>();
        String[] columns1 =strDf.columns();
        for (int i=0;i<columns1.length;i++){
            String columnIndex = columns1[i]+"_index";
            String columnVoc = columns1[i]+"_voc";
            indexColumns.add(columnIndex);
            vocColumns.add(columnVoc);
            StringIndexer indexer = new StringIndexer().setInputCol(strDf.columns()[i]).setOutputCol(columnIndex);
            strDf = indexer.fit(strDf).transform(strDf);
        }
        OneHotEncoderEstimator encoder = new OneHotEncoderEstimator().setInputCols(indexColumns.toArray(new String[0]))
                .setOutputCols(vocColumns.toArray(new String[0]));
        OneHotEncoderModel model = encoder.fit(strDf);
        Column[] columns2 = vocColumns.stream().map(functions::col).toArray(Column[]::new);
        Dataset<Row> encoderDf = model.transform(strDf).select(columns2);
        // 将由字符类型变量转化的哑变量与数值变量合并为一个dataframe
        Dataset<Row> allDf = encoderDf.crossJoin(numberDf);

        // 划分特征和标签 (注：数组通过Arrays.asList直接转为list后，该list不能进行修改元素操作)
        List<String> features = new ArrayList<String>(Arrays.asList(allDf.columns()));
        features.remove("SCT");
        // stage
        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(features.toArray(new String[0])).setOutputCol("features");

        Dataset<Row> vectorDf = vectorAssembler.transform(allDf).select("SCT","features");
        // 识别分类特征判断是否为连续
//        VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(vectorDf);
        // 划分训练集和测试集

        Dataset<Row>[] splitsDf = vectorDf.randomSplit(new double[]{0.8,0.2});
        Dataset<Row> trainData = splitsDf[0];
        Dataset<Row> testData = splitsDf[1];
        long timeTrain = System.currentTimeMillis();
        System.out.println("模型开始训练");
        GBTRegressor gbt  = new GBTRegressor().setLabelCol("SCT").setFeaturesCol("features").setMaxIter(10);
        GBTRegressionModel gbtModel = gbt.fit(trainData);
        System.out.println("model train use time:"+(System.currentTimeMillis()-timeTrain)/1000);
        Dataset<Row> lrPrediction = gbtModel.transform(testData);
        lrPrediction.show(100);
        RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("SCT").setMetricName("r2").setPredictionCol("prediction");
        System.out.println(evaluator.evaluate(lrPrediction));
        System.out.println((System.currentTimeMillis()-time01)/1000);
        // 模型持久化到本地
        gbtModel.save("GBTModel");
    }
}

