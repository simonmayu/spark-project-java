package org.simon.project.mobileAppLog;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Demo:移动APP访问流量日志分析案例
 * 
 * 需求:	
 * 		1.根据每个移动端上行流量和下行流量统计每个移动端总上行流量 和总下行流量
 *      2.结果按照倒序排序
 *      3.获取流量最大的前10个设备
 *
 * 技术实现:
 * 		1.二次排序(自定义排序Key):
 * 			优先按照上行流量进行排序,如果是上行流量相等 按照下行流量排序，如果下行流量相等按照时间戳
 * 		2.数据聚合:
 * 			按照设备deviceID进行数据聚合
 * 
 * 
 * 日志数据格式
 * 			timestamp		deviceID							upTraffic  downTraffic
 *      	1454307391161	77e3c9e1811d4fb291d0d9bbd456bb4b	79976	   11496
 * 
 * @author simon
 * 
 */
public class AppLogAnalyze {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
			.setAppName("AppLogAnalyze")
			.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 这里的path 就是 DataGenarate 数据生成的地址 
		JavaRDD<String> accessLogRDD = sc.textFile("");
		
		JavaPairRDD<String,AccessLogInfo> accessLogPairRDD = mapAccessLogRDD2Pair(accessLogRDD);
		
		JavaPairRDD<String,AccessLogInfo> aggrAccessLogPairRDD = aggregateByDeviceID(accessLogPairRDD);
		
		JavaPairRDD<AccessLogSortKey,String> accessLogSortRDD = mapRDDKey2SortKey(aggrAccessLogPairRDD);
		
		JavaPairRDD<AccessLogSortKey,String> sortedAccessLogRDD = accessLogSortRDD.sortByKey(false);
	
		List<Tuple2<AccessLogSortKey,String>> top10Datalist = sortedAccessLogRDD.take(10);
		
		// 这里只是简单的打印出来
		for(Tuple2<AccessLogSortKey,String> tuple:top10Datalist){
			System.out.println(tuple._2 + ": " + tuple._1);  
		}
		
		sc.close();
	}

	

	/**
	 * 
	 * 将日志映射为key-value的格式
	 * 
	 * @param accessLogRDD
	 * @return key-value格式RDD
	 */
	private static JavaPairRDD<String, AccessLogInfo> mapAccessLogRDD2Pair(
			JavaRDD<String> accessLogRDD) {
		
		return accessLogRDD.mapToPair(new PairFunction<String, String, AccessLogInfo>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, AccessLogInfo> call(String line)
					throws Exception {
				String[] accessLogSplited = line.split(" ");
				
				long timestamp = Long.valueOf(accessLogSplited[0]);
				String deviceID = accessLogSplited[1];
				long upTraffic = Long.valueOf(accessLogSplited[2]);
				long downTraffic = Long.valueOf(accessLogSplited[3]);
				
				AccessLogInfo accessLogInfo = new AccessLogInfo(timestamp, upTraffic, downTraffic);
				
				return new Tuple2<String,AccessLogInfo>(deviceID,accessLogInfo);
			}
		});
	}
	
	/**
	 * 根据deviceID进行聚合操作
	 * 计算出每个deviceID的总上行流量,总下行流量以及最早访问时间
	 * @param accessLogPairRDD
	 * @return
	 */
	private static JavaPairRDD<String, AccessLogInfo> aggregateByDeviceID(
			JavaPairRDD<String, AccessLogInfo> accessLogPairRDD) {
		return accessLogPairRDD.reduceByKey(new Function2<AccessLogInfo, AccessLogInfo, AccessLogInfo>() {
			
			private static final long serialVersionUID = 1L;

			public AccessLogInfo call(AccessLogInfo accessLogInfo1, AccessLogInfo accessLogInfo2)
					throws Exception {
				long timestamp = accessLogInfo1.getTimestamp() < accessLogInfo2.getTimestamp()?
							accessLogInfo1.getTimestamp():accessLogInfo2.getTimestamp();
				long upTraffic = accessLogInfo1.getUpTraffic() + accessLogInfo2.getUpTraffic();
				long downTraffic = accessLogInfo1.getUpTraffic() + accessLogInfo2.getDownTraffic();
				AccessLogInfo accessLogInfo = new AccessLogInfo(timestamp, upTraffic, downTraffic);
				return accessLogInfo;
			}
		});
	}
	/**
	 * 将
	 * @param aggrAccessLogPairRDD
	 * @return
	 */
	private static JavaPairRDD<AccessLogSortKey, String> mapRDDKey2SortKey(
			JavaPairRDD<String, AccessLogInfo> aggrAccessLogPairRDD) {
		return aggrAccessLogPairRDD.mapToPair(new PairFunction<Tuple2<String,AccessLogInfo>, AccessLogSortKey, String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<AccessLogSortKey, String> call(
					Tuple2<String, AccessLogInfo> tuple) throws Exception {
				
				String deviceID = tuple._1;
				AccessLogInfo accessLogInfo = tuple._2;
				
				AccessLogSortKey accessLogSortKey = new AccessLogSortKey();
				accessLogSortKey.setTimestamp(accessLogInfo.getTimestamp());
				accessLogSortKey.setUpTraffic(accessLogInfo.getUpTraffic());
				accessLogSortKey.setDownTraffic(accessLogInfo.getDownTraffic());
				
				return new Tuple2<AccessLogSortKey,String>(accessLogSortKey,deviceID);
			}
		});
	}
	
}
