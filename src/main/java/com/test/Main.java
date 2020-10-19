package com.test;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple1;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;


/**
 * При работе столкнулся с проблеммой невозможности сделать вложеный цикл
 * или воспользоваться неким внешним буфером позволяющим хранить данные.
 * В итоге для предачи и хранения данных между иттерациями цикла использовал reduce и
 * некую переменную встроеную внутрь коллекци которую я пребирал, данно ерешение не считаю хорошим,
 * но другово не вижу.
 */
public class Main
{



    public static void main(String[] args)
    {




        ArrayList<Tuple2<Long, Long>> calls =new  ArrayList<Tuple2<Long, Long>>();
        calls.add(new Tuple2(1l, 2l));
        calls.add(new Tuple2(3l, 5l));
        calls.add(new Tuple2(9l, 1l));
        calls.add(new Tuple2(8l, 9l));
        calls.add(new Tuple2(1l, 120l));
        calls.add(new Tuple2(5l, 6l));
        calls.add(new Tuple2(1l, 3l));
        calls.add(new Tuple2(1l, 20l));


        //вводим данные
        SparkConf sparkConf = new SparkConf().setAppName("Spark hi").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Tuple2<Long, Long>> callRdd=sparkContext.parallelize(calls);


        //преобразуем данные в формат содержащий выходные/промежуточные данные
        JavaRDD<Tuple3<Long, Long, HashMap<Long, HashSet<Long>>>> connections = callRdd.distinct().//сжимаем входные данные
                map(call ->
        {
            Tuple3<Long, Long, HashMap<Long, HashSet<Long>>> tuple =
                    new Tuple3(call._1, call._2, new HashMap<Long, HashSet<Long>>());
            return tuple;
        });

        //распечатываем данные (для удобства просмотра вывода)
        System.out.println("connections:");
        connections.foreach(item->System.out.println("A: "+ item._1()+" B:"+item._2() + " Connections: "+ item._3()));

        //производим окончательные вычисления.
        Tuple3<Long, Long, HashMap<Long, HashSet<Long>>> ans = connections.reduce((a, b) -> {
            Tuple3<Long, Long, HashMap<Long, HashSet<Long>>> tuple = new Tuple3<Long, Long, HashMap<Long, HashSet<Long>>>(0L, 0L,a._3());
            b._3().forEach((k,v)-> {
                if(tuple._3().containsKey(k))
                {
                    tuple._3().get(k).addAll(v);
                }
                else
                {
                    tuple._3().put(k,v);
                }
            });




            tuple._3().putIfAbsent(a._1(), new  HashSet<Long>());
            tuple._3().putIfAbsent(b._1(), new  HashSet<Long>());

            tuple._3().get(a._1()).add(a._2());
            tuple._3().get(b._1()).add(b._2());

            return tuple;
        });
        System.out.println("Anser: " + ans);
        sparkContext.close();



    }




}
