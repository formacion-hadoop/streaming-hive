package fhadoop;

import java.util.Calendar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fhadoop.utils.Log;


public class Streaming {
    static final Logger logger = LoggerFactory.getLogger(Streaming.class);


    public static void main(String[] args) {

 
        JavaStreamingContext jssc = null;

            SparkConf sparkConf = new SparkConf().setAppName("Formacion Hadoop Streaming");
            jssc = new JavaStreamingContext(sparkConf,
                    new Duration(30000));
           
            JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999,
                    StorageLevels.MEMORY_AND_DISK_SER);

            JavaDStream<String> words = lines.map(new Function<String, String>() {
                /**
                 * 
                 */
                private static final long serialVersionUID = -7185010247510276055L;

                @Override
                public String call(String arg0) throws Exception {

                    return arg0;
                }
            });

            words.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
                /**
                 * 
                 */
                private static final long serialVersionUID = 1L;

                @Override
                public void call(JavaRDD<String> rdd, Time time) throws Exception {

                    HiveContext sqlContext = null;
                    if (sqlContext == null) {
                        logger.info("HiveContext: Creando contexto de Hive");
                        sqlContext = new HiveContext(rdd.context());

                    }

                    JavaRDD<Log> rowRDD = rdd.map(new Function<String, Log>() {
                        /**
                         * 
                         */
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Log call(String line) throws Exception {
                            Log record = new Log();
                            record.setLog(line);
                            record.setYear(27);
                            record.setMonth(05);
                            record.setDay(2017);
                            return record;
                        }
                    });
                    sqlContext.setConf("hive.exec.dynamic.partition", "true");
                    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
                    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");



                    String hiveTable = "fhadoop.logs";
                    DataFrame wordsDataFrame = sqlContext.createDataFrame(rowRDD, Log.class);
                    wordsDataFrame.write().partitionBy("year", "month", "day").mode(SaveMode.Append)
                            .insertInto(hiveTable);
                    
                    Calendar now = Calendar.getInstance();
                    if (now.get(Calendar.HOUR_OF_DAY) == 11) {
                        
                        logger.info("Iniciando el proceso de compactación");
                        // String fecha = toDate();
                        String fecha = "2017/5/27";
                        wordsDataFrame = sqlContext
                                .sql("select * from " + hiveTable + " where year=" + fecha.split("/")[0]
                                        + " and month=" + fecha.split("/")[1] + " and day=" + fecha.split("/")[2])
                                .coalesce(1);
                        wordsDataFrame.write().partitionBy("year", "month", "day").mode(SaveMode.Overwrite)
                                .insertInto(hiveTable);
                        logger.info("Finalizado el proceso de compactación");
                    }
                }
            });

            // Start the computation
            jssc.start();
            jssc.awaitTermination();

  

    }


}
