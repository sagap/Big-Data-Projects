import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

public class Task4 {
    
    public static void main(String[] args)
    {
        if(args.length != 3)
        {
            System.out.println("You should provide 3 parameters!");
            return;
        }
        String inputFileCustomer = args[0];
        String inputFileOrders = args[1];
        String outputFile = args[2];
        
        String master = "local[4]";

            SparkConf conf = new SparkConf()
            .setAppName(Task4.class.getName())
            .setMaster(master);
            
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> orders = sc.textFile(inputFileOrders);
        JavaRDD<String> customers = sc.textFile(inputFileCustomer);
        
        // for windows
        //System.setProperty("hadoop.home.dir", "C:\\winutils");
        
        JavaPairRDD key_customers = customers.mapToPair((String x) ->new Tuple2(x.split("\\|")[0],""));

        JavaPairRDD key_orders = orders.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String x) throws Exception {
                String[] str = x.split("\\|");
                if(str.length!=9)     // if no O_COMMENT
                {
                    return new Tuple2(str[1],"");
                }
                else{
                    return new Tuple2(str[1],str[8]+"|");
                }
            }
        });
        JavaPairRDD map_orders = key_orders.reduceByKey(new Function2<String,String,String>() {
            @Override
            public String call(String t1, String t2) throws Exception {
                return t1+t2;
            }
        });
        JavaPairRDD part_orders = map_orders.partitionBy(new HashPartitioner(120));
        JavaPairRDD part_customers = key_customers.partitionBy(new HashPartitioner(120));
        //Broadcast<Map<String,String>> broadcastOrders = sc.broadcast(map_orders);
                
        part_orders.persist(StorageLevel.MEMORY_ONLY());
        part_customers.persist(StorageLevel.MEMORY_ONLY());
        JavaRDD joinedRDD = part_customers.zipPartitions(part_orders, new FlatMapFunction2<Iterator,Iterator,Iterator>() {
            @Override
            public Iterator call(Iterator customers, Iterator orders) throws Exception {
                Iterator<Tuple2<String,String>> it3 = customers;
                Iterator<Tuple2<String,String>> it4 = orders;
                HashMap hashM = new HashMap();
                while(it3.hasNext())
                {
                    Tuple2 cust =it3.next();
                    hashM.put(cust._1, cust._2);       
                }
                ArrayList<Tuple2<String,String>> lista = new ArrayList<>();
                while(it4.hasNext())
                {
                    Tuple2 order =it4.next();
                    if(hashM.containsKey(order._1))       // join logic is here
                        lista.add(order);
                }
                return lista.iterator();
            }
        });
        JavaPairRDD temp = joinedRDD.mapToPair(new PairFunction<Tuple2<String,String>,String,String[]>() {
            @Override
            public Tuple2<String, String[]> call(Tuple2<String, String> x) throws Exception {
                String[] str = x._2.split("\\|");
                String[] str2 = new String[str.length];
                for(int i=0;i<str.length;i++) 
                {    
                    String s = str[i];
                    str2[i] = x._1+","+s;
                }
                return new Tuple2(x._1,str2);
            }
        }); 
        JavaRDD<String[]> temp2 = temp.values();
        JavaRDD result = temp2.map(new Function<String[],String>() {
            @Override
            public String call(String[] x) throws Exception {
                return String.join("\n", x);
            }
        });
        
        result.coalesce(1).saveAsTextFile("tempFile");
        FileFilter urlFilter = new FileFilter() {
        @Override
        public boolean accept(File file) {
            if (file.isDirectory()) {
                return true; // return directories for recursion
            }
            return file.getName().startsWith("part-00");
        };
        };
        File dir = new File("tempFile");
        File[] files = dir.listFiles(urlFilter);
    try
   {
    // savePath is the path of the output file
    FileOutputStream outFile = new FileOutputStream(outputFile);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outFile));
    for (File file : files)
    {
        FileInputStream inFile = new FileInputStream(file);
        /*        Integer b = null;
                while ((b = inFile.read()) != -1)
                outFile.write(b); */
            BufferedReader reader = new BufferedReader(new InputStreamReader(inFile));
            String line = reader.readLine();
            while(line != null){
                bw.write(line+"\n");
                line = reader.readLine();
            }         
        inFile.close();
    }
      bw.close();
      outFile.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
        
    }   
} 
