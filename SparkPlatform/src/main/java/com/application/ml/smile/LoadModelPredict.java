package com.application.ml.smile;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;
import org.apache.commons.csv.CSVFormat;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.measure.NominalScale;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;
import smile.feature.SparseOneHotEncoder;
import smile.io.Read;
import smile.regression.LinearModel;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


/**
 * @author 张睿
 * @create 2020-05-09 16:39
 **/
public class LoadModelPredict {
    private List<Object> featureDataList;
    private String linerModelPath;
    private String allDataPath;

    public void setFeatureDataList(List<Object> featureDataList) {
        this.featureDataList = featureDataList;
    }

    public void setLinerModelPath(String linerModelPath) {
        this.linerModelPath = linerModelPath;
    }

    public void setAllDataPath(String allDataPath) {
        this.allDataPath = allDataPath;
    }

    public double[] doPredict() throws IOException {
        // 模型加载
        XStream xStream = new XStream(new DomDriver());
        xStream.alias("LinearModel", LinearModel.class);
        xStream.setClassLoader(LinearModel.class.getClassLoader());
        FileInputStream fileInputStream = new FileInputStream(linerModelPath);
        LinearModel linearModel = (LinearModel) xStream.fromXML(fileInputStream);
        // 数据处理
        String[] cites = {"黄石", "南京", "枣庄", "儋州", "宝鸡", "北海", "岳阳", "韶关", "本溪", "徐州", "临沂", "怀化",
                "张家界", "长沙", "张家口", "沈阳", "龙岩", "西安", "长治", "榆林", "丽江", "连云港", "临汾", "荆门", "清远",
                "赤峰", "洛阳", "衡阳", "沧州", "无锡", "潮州", "钦州", "南通", "泰州", "邢台", "新乡", "盐城", "许昌", "湖州",
                "河源", "日照", "开封", "大连", "桂林", "赣州", "锦州", "营口", "西昌", "淮安", "遂宁", "景洪", "苏州", "兰州",
                "揭阳", "昆明", "汕头", "包头", "威海", "杭州", "遵义", "十堰", "烟台", "江门", "台州", "承德", "南充", "宿迁",
                "太原", "天津", "保定", "扬州", "湛江", "齐齐哈尔", "成都", "嘉兴", "德州", "郑州", "宜昌", "泉州", "襄阳", "达州",
                "晋城", "自贡", "惠州", "中山", "肇庆", "舟山", "汕尾", "鄂尔多斯", "宁波", "泸州", "绍兴", "厦门", "青岛", "福州",
                "茂名", "三明", "珠海", "咸阳", "淮南", "金华", "辽阳", "雅安", "贵港", "济南", "淄博", "德阳", "廊坊", "延吉",
                "合肥", "石家庄", "东莞", "银川", "湘潭", "深圳", "三亚", "温州", "重庆", "衢州", "常德", "大理", "济宁", "大同",
                "南昌", "东营", "云浮", "晋中", "马鞍山", "曲靖", "聊城", "梅州", "丽水", "荆州", "呼和浩特", "九江", "西宁", "阳江",
                "内江", "眉山", "南宁", "漳州", "郴州", "绵阳", "潍坊", "莆田", "上海", "唐山", "海口", "邯郸", "衡水", "泰安",
                "牡丹江", "运城", "镇江", "长春", "哈尔滨", "贵阳", "乐山", "北京", "蚌埠", "广州", "盘锦", "柳州", "大庆", "丹东",
                "宁德", "佛山", "芜湖", "攀枝花", "普洱", "鞍山", "常州", "宜宾", "吉林", "安庆", "秦皇岛", "武汉", "渭南"};

        String[] businessType={"C3","CS3","C1","CS2","C2","O","O+","S","TS","R","R+","CBD","CS1","T","C3+","C2+","S+","T+","X","TS+","CS2+","CS3+","C2-","Other","C1-"};
        String[] businessAreaSubtype = {"District","Prime","O","O+","R","Airport","R+","Other","Railway","Cinema","T","Uni","OL","Hosp","REBA","Highway","Metro","Passby",
                "Hotel","Others","T+","Cul"};
        List<StructField> structFieldList = new ArrayList<>();
        structFieldList.add(new StructField("big_region", DataTypes.ByteType,new NominalScale("CW","N","S","EN","ES","SH")));
        structFieldList.add(new StructField("small_region",DataTypes.ByteType,new NominalScale("N1","S1","W","EN","C","ES","N2","S2","SH")));
        structFieldList.add(new StructField("city_name",DataTypes.ShortType,new NominalScale(cites)));
        structFieldList.add(new StructField("city_level",DataTypes.ByteType,new NominalScale("1","2+","3","5","2","4")));
        structFieldList.add(new StructField("DESIGN_TYPE",DataTypes.ByteType,new NominalScale("Core","High Profile","High Profile Plus","Flagship")));
        structFieldList.add(new StructField("FORMAT_TYPE",DataTypes.ByteType,new NominalScale("无", "Community", "R+", "Starbucks Now", "Pet")));
        structFieldList.add(new StructField("PORTFOLIO_TYPE",DataTypes.ByteType,new NominalScale("Core (Standard)", "Reserve Bar", "Reserve Only")));
        structFieldList.add(new StructField("PROGRAM_TYPE",DataTypes.ByteType,new NominalScale("w/o Brew bar","Brew bar","Pour-over","Espresso beverage (w/o BE)","Espresso beverage (w BE)")));
        structFieldList.add(new StructField("OPERATION_TYPE",DataTypes.ByteType,new NominalScale("Café","Kiosk","Hidden Kitchen","Express")));
        structFieldList.add(new StructField("DELIVERY_TYPE",DataTypes.ByteType,new NominalScale("Delivery (3rd pl)","无","Delivery Express (Mop)","Hema")));
        structFieldList.add(new StructField("SPECIAL_EQUIPMENT",DataTypes.ByteType,new NominalScale("无","Affogato","Siphon","Nitro","Black Eagle")));
        structFieldList.add(new StructField("IS_CORE_SITE",DataTypes.ByteType,new NominalScale("否","是")));
        structFieldList.add(new StructField("mini_market_level",DataTypes.ByteType,new NominalScale("1","2","3","4","5")));
        structFieldList.add(new StructField("business_type",DataTypes.ByteType,new NominalScale(businessType)));
        structFieldList.add(new StructField("business_level",DataTypes.ByteType,new NominalScale("1","2","3","4")));
        structFieldList.add(new StructField("business_area_type",DataTypes.ByteType,new NominalScale("T", "O", "Other", "C", "TS", "R")));
        structFieldList.add(new StructField("business_area_subtype",DataTypes.ByteType,new NominalScale(businessAreaSubtype)));
        structFieldList.add(new StructField("store_location",DataTypes.ByteType,new NominalScale("in-line","corner","in-mall","exterior","free-standing","other")));
        structFieldList.add(new StructField("total_usable_area",DataTypes.DoubleType));
        structFieldList.add(new StructField("warehouse_area",DataTypes.DoubleType));
        structFieldList.add(new StructField("door_head_width",DataTypes.DoubleType));
        structFieldList.add(new StructField("store_deep",DataTypes.DoubleType));
        structFieldList.add(new StructField("tenancy",DataTypes.DoubleType));
        structFieldList.add(new StructField("ADT",DataTypes.IntegerType));
        structFieldList.add(new StructField("SCT",DataTypes.DoubleType));
        StructType structType = new StructType(structFieldList);

        List<StructField> strStructFieldList = new ArrayList<>();
        strStructFieldList.add(new StructField("big_region",DataTypes.ByteType,new NominalScale("CW","N","S","EN","ES","SH")));
        strStructFieldList.add(new StructField("small_region",DataTypes.ByteType,new NominalScale("N1","S1","W","EN","C","ES","N2","S2","SH")));
        strStructFieldList.add(new StructField("city_name",DataTypes.ByteType,new NominalScale(cites)));
        strStructFieldList.add(new StructField("city_level",DataTypes.ByteType,new NominalScale("1","2+","3","5","2","4")));
        strStructFieldList.add(new StructField("DESIGN_TYPE",DataTypes.ByteType,new NominalScale("Core","High Profile","High Profile Plus","Flagship")));
        strStructFieldList.add(new StructField("FORMAT_TYPE",DataTypes.ByteType,new NominalScale("无", "Community", "R+", "Starbucks Now", "Pet")));
        strStructFieldList.add(new StructField("PORTFOLIO_TYPE",DataTypes.ByteType,new NominalScale("Core (Standard)", "Reserve Bar", "Reserve Only")));
        strStructFieldList.add(new StructField("PROGRAM_TYPE",DataTypes.ByteType,new NominalScale("w/o Brew bar","Brew bar","Pour-over","Espresso beverage (w/o BE)","Espresso beverage (w BE)")));
        strStructFieldList.add(new StructField("OPERATION_TYPE",DataTypes.ByteType,new NominalScale("Café","Kiosk","Hidden Kitchen","Express")));
        strStructFieldList.add(new StructField("DELIVERY_TYPE",DataTypes.ByteType,new NominalScale("Delivery (3rd pl)","无","Delivery Express (Mop)","Hema")));
        strStructFieldList.add(new StructField("SPECIAL_EQUIPMENT",DataTypes.ByteType,new NominalScale("无","Affogato","Siphon","Nitro","Black Eagle")));
        strStructFieldList.add(new StructField("IS_CORE_SITE",DataTypes.ByteType,new NominalScale("否","是")));
        strStructFieldList.add(new StructField("mini_market_level",DataTypes.ShortType,new NominalScale("1","2","3","4","5")));
        strStructFieldList.add(new StructField("business_type",DataTypes.ByteType,new NominalScale(businessType)));
        strStructFieldList.add(new StructField("business_level",DataTypes.ShortType,new NominalScale("1","2","3","4")));
        strStructFieldList.add(new StructField("business_area_type",DataTypes.ByteType,new NominalScale("C","R","O","Other","TS","T")));
        strStructFieldList.add(new StructField("business_area_subtype",DataTypes.ByteType,new NominalScale(businessAreaSubtype)));
        strStructFieldList.add(new StructField("store_location",DataTypes.ByteType,new NominalScale("in-line","corner","in-mall","exterior","free-standing","other")));
        StructType strStructType = new StructType(strStructFieldList);
        // 在预测数据集中添加SCT值，转为dataframe格式
        featureDataList.add(0);
        List<Tuple> tupleList = new ArrayList<>();
        List<Function<String,Object>> parser = structType.parser();
        StructField[] fields = structType.fields();
        Object[] row = new Object[fields.length];

        for (int i=0;i<featureDataList.size();++i){
            String a = featureDataList.get(i).toString();
            if (!a.isEmpty()){
                row[i] = ((Function)parser.get(i)).apply(a);
            }
        }

        tupleList.add(Tuple.of(row,structType));
        StructType structType1 = structType.boxed(tupleList);
        DataFrame featureDf = DataFrame.of(tupleList,structType1);
        System.out.println(featureDf);
        // 合并元素
        Path path = Paths.get(allDataPath);
        CSVFormat csvFormat = CSVFormat.DEFAULT;
        DataFrame dataDf01 = Read.csv(path,csvFormat,structType);
        System.out.println(dataDf01);
        DataFrame dataDf = dataDf01.union(featureDf);
        // encoder
        String[] strColumns = {"big_region","small_region","city_name","city_level","DESIGN_TYPE","FORMAT_TYPE","PORTFOLIO_TYPE","PROGRAM_TYPE",
                "OPERATION_TYPE","DELIVERY_TYPE","SPECIAL_EQUIPMENT","IS_CORE_SITE","mini_market_level","business_type","business_level",
                "business_area_type","business_area_subtype","store_location"};
        String[] numberColumns = {"total_usable_area","warehouse_area","door_head_width","store_deep","tenancy","ADT","SCT"};
        DataFrame strDf = dataDf.select(strColumns);
        SparseOneHotEncoder sparseOneHotEncoder = new SparseOneHotEncoder(strStructType);
        int[][] oneHotData = sparseOneHotEncoder.apply(strDf);
        DataFrame encoderDf = DataFrame.of(oneHotData,strColumns).merge(dataDf.select(numberColumns));
        return linearModel.predict(encoderDf.of(encoderDf.size()-1));
    }

    public static void main(String[] args) throws IOException {

        List<Object> features = new ArrayList<>();
        features.add("CW");
        features.add("C");
        features.add("西安");
        features.add("2+");
        features.add("Core");
        features.add("无");
        features.add("Core (Standard)");
        features.add("Brew bar");
        features.add("Café");
        features.add("Delivery (3rd pl)");
        features.add("无");
        features.add("是");
        features.add("2");
        features.add("C2");
        features.add("2");
        features.add("C");
        features.add("Prime");
        features.add("corner");
        features.add(174.0);
        features.add(0.0);
        features.add(14.3);
        features.add(16.0);
        features.add(12.0);
        features.add(160);
        String linerModelPath = "D:\\rongze\\data\\linearModel";
        String allDataPath = "D:\\rongze\\data\\SCT___a___v4_prepared_prepared.csv";
        LoadModelPredict loadModelPredict = new LoadModelPredict();
    }
}

