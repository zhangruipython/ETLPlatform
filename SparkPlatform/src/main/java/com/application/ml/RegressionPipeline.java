package com.application.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author 张睿
 * @create 2020-05-08 11:17
 * 构造回归模型训练Pipeline
 * Pipeline 包含3个stage ==> 字符变量向量化 OneHotEncoder ==> VectorAssembler 统一所有变量为一个futures变量==> gbt模型参数初始化
 * 思路变化： 不再划分将dataframe拆分为数值类型变量的df和字符类型变量的df,而是在完整的dataframe上操作
 * dataframe 尽量不落地，通过构建stage，在stage中读取列进行操作
 **/
public class RegressionPipeline {
    public static void main(String[] args) throws IOException {
        long time01 = System.currentTimeMillis();
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Pipeline").config(new SparkConf().setMaster("local[*]")).getOrCreate();
        // 读取csv中模型训练数据
        Dataset<Row> dataDf = spark.read().format("csv").option("header","true").option("sep", ",")
                .option("inferSchema", "true").load("D:/rongze/notebook/SCT___a___v4_prepared.csv").cache();
        dataDf.show();
        // 找出字符类型变量
        String str = "StringType";
        List<String> strColumns = new ArrayList<String>();
        dataDf.schema().foreach(s->{
            // 判断列属性
            if(str.equals(s.dataType().toString())){
                strColumns.add(s.name());
            }
            return strColumns;
        });
        List<String> numberColumns = Arrays.stream(dataDf.columns()).filter(item->!strColumns.contains(item)).collect(Collectors.toList());
        numberColumns.remove("SCT");
        System.out.println(numberColumns);

        // 将字符类型变量转为数值类型，并不断添加至dataframe
        List<String> indexColumns = new ArrayList<>();
        List<String> vocColumns = new ArrayList<>();
        for (String strColumn : strColumns) {
            String columnIndex = strColumn + "_index";
            String columnVoc = strColumn + "_voc";
            indexColumns.add(columnIndex);
            vocColumns.add(columnVoc);
            StringIndexer indexer = new StringIndexer().setInputCol(strColumn).setOutputCol(columnIndex);
            dataDf = indexer.fit(dataDf).transform(dataDf);
        }

        // stage01 字符变量==>数值索引变量==>哑变量
        OneHotEncoderModel oneHotModel = new OneHotEncoderEstimator().setInputCols(indexColumns.toArray(new String[0]))
                .setOutputCols(vocColumns.toArray(new String[0])).fit(dataDf);

        // stage02 all features=>feature
        numberColumns.addAll(vocColumns);
        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(numberColumns.toArray(new String[0])).setOutputCol("features");

        // stage03 初始化回归函数
        GBTRegressor gbt  = new GBTRegressor().setLabelCol("SCT").setFeaturesCol("features").setMaxIter(10);

        // 构建Pipeline
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{oneHotModel,vectorAssembler,gbt});

        // 模型训练
        Dataset<Row>[] splitDf = dataDf.randomSplit(new double[]{0.8,0.2});
        PipelineModel pipelineModel = pipeline.fit(splitDf[0]);
        System.out.println("model train use time:"+(System.currentTimeMillis()-time01)/1000);

        // 模型评估
        Dataset<Row> prediction = pipelineModel.transform(splitDf[1]);
        prediction.select("SCT","prediction","features").show(100);
        RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("SCT").setMetricName("r2").setPredictionCol("prediction");
        System.out.println(evaluator.evaluate(prediction));

        // 模型持久化
        pipelineModel.save("GBTPipelineModel");

        // 模型调用
        long time02 = System.currentTimeMillis();
        PipelineModel gbtPipelineModel = PipelineModel.load("D:\\MyProject\\SparkPlatform\\GBTPipelineModel");
        Dataset<Row> prediction1 = gbtPipelineModel.transform(splitDf[1]);
        System.out.println("查看持久化pipeline模型调用效果");
        prediction1.select("SCT","prediction").show();
        System.out.println("调用持久化pipeline模型预测耗时："+(System.currentTimeMillis()-time02)/1000);
    }
}

