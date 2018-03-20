import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class CompressedColumnStore implements Serializable{
    
    final private JavaPairRDD[] attributes;
    private String[] attr_names;
    private String[] attr_types;
    private static final Map<String,Integer> operations;
    
      private Map dictionary;
   // If true, the current page is dictionary encoded.
    private boolean isDictionaryEncoded = false;
    private int colEncoded = -1;
    JavaPairRDD indexedCol = null;
    
    // here are the allowed operations
    static {    
        Map<String,Integer> aMap = new HashMap<>();
        aMap.put("=",0);
        aMap.put("<",1);
        aMap.put("<=",2);
        aMap.put(">",3);
        aMap.put(">=",4);
        operations = Collections.unmodifiableMap(aMap);
    }
    
    public CompressedColumnStore(String[] schema) {
        this.attributes = new JavaPairRDD[schema.length];
        attr_names = new String[schema.length];
        attr_types = new String[schema.length];
        for(int i =0;i<schema.length;i++)
        {    
            String[] temp = schema[i].split("\\:");
            attr_names[i] = temp[0];
            attr_types[i] = temp[1];    
        }
        dictionary = null;

    }
    
    public JavaPairRDD performSelection(String[] selection)
    {
        int col;
        JavaPairRDD result = null;
        if((col = this.findCorrespondingCol(selection[0])) != -1)
        {
            Integer op;
            if((op = operations.get(selection[1])) != null)
            {
                if(col == this.colEncoded && this.isDictionaryEncoded)
                    result = this.performOperationWithIndex(col, op, selection[2]);
                else
                    result = this.performOperation(col, op, selection[2]);
            }
        }
        return result;
    }
    
    public JavaPairRDD[] getAttributes() {
        return attributes;
    }

    public String[] getAttr_names() {
        return attr_names;
    }

    public void setAttr_names(String[] attr_names) {
        this.attr_names = attr_names;
    }

    public String[] getAttr_types() {
        return attr_types;
    }

    public void setAttr_types(String[] attr_types) {
        this.attr_types = attr_types;
    }
    
    public void setSpecificRDD(JavaPairRDD temp, int pos)
    {
        this.attributes[pos] = temp.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer,String>(){
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> x) throws Exception {
                return new Tuple2(x._2,x._1);
            }
        });
    }
    
    public JavaPairRDD getSpecificRDD(int pos)
    {
        return this.attributes[pos];
    }
    
    // returns the number that matches the column name to find the rdd for the column
    public int findCorrespondingCol(String col)
    {
        for(int i=0;i<this.getAttr_names().length;i++)
            if(attr_names[i].compareTo(col)==0)
                return i;
        return -1;
    }
    
    public JavaPairRDD performOperation(int col, Integer op, String val)
    {   
        JavaPairRDD column = this.getSpecificRDD(col);
        JavaPairRDD toR = null;
        JavaPairRDD alf = null;
        if(this.attr_types[col].compareTo("Int") == 0)    
        {
            alf = column.mapToPair(new PairFunction<Tuple2<Integer,String>, Integer, String>() {
                public Tuple2<Integer,String> call(Tuple2<Integer,String> x) 
                {    
                    int y = Integer.parseInt(val);
                    int x1 = Integer.parseInt(x._2);
                    Tuple2<Integer,String> xn = new Tuple2<>(x._1,"");
                    switch(op){
                        case 0 :   
                            if (x1 == y){
                                return xn;
                            }
                            else return null;
                        case 1 : 
                            if (x1 < y)
                                return xn;
                            else return null;
                        case  2 : 
                            if(x1 <= y)
                                return xn;
                            else return null;
                        case 3 :
                            if(x1 > y)
                                return xn;
                            else return null;
                        case 4 :
                            if(x1>= y)
                                return xn;
                            else return null;
                        default:
                                return null;
                    }
                }
            });
        }
        else if(this.attr_types[col].compareTo("Float") == 0)
        {
            alf = column.mapToPair(new PairFunction<Tuple2<Integer,String>,Integer,String>() {
                public Tuple2<Integer,String> call(Tuple2<Integer,String> x) 
                {    
                    Float x1 = Float.parseFloat(x._2);
                    Float y = Float.parseFloat(val);
                    Tuple2<Integer,String> xn = new Tuple2<>(x._1,"");
                    switch(op){
                        case 0 :   
                     if(x1.equals(y))
                         return xn;
                     else return null;
                        case 1 : 
                    if(x1 < y) return xn;
                        else return null;
                        case  2 : 
                    if(x1 < y || x1.equals(y))return xn;
                    else return null;
                        case 3 :
                    if(x1 > y) return xn;
                    else return null;
                        case 4 :
                    if(x1 > y || x1.equals(y)) return xn;
                    else return null;
                        default:
                                return null;
                    }
                }
            });
        }
        else if(this.attr_names[col].contains("DATE"))
            {
                alf = column.mapToPair(new PairFunction<Tuple2<Integer,String>, Integer, String>() {
                    public Tuple2<Integer,String> call(Tuple2<Integer,String> x) 
                    {    
                        LocalDate y = LocalDate.parse(val);
                        LocalDate a = LocalDate.parse(x._2);
                        Tuple2<Integer,String> xn = new Tuple2<>(x._1,"");
                        switch(op){
                            case 0 :   
                        if (a.equals(y)) return xn;
                        else return null;
                            case 1 : 
                        if(a.isBefore(y))return xn;
                        else return null;
                            case  2 : 
                        if(a.equals(y) || a.isBefore(y))return xn;
                        else return null;
                            case 3 :
                        if(a.isAfter(y))return xn;
                        else return null;
                            case 4 :
                        if(a.isAfter(y)|| a.equals(y))return xn;
                        else return null;
                            default:
                                    return null;
                        }
                    }
                });
            }
            else 
            {
                alf = column.mapToPair(new PairFunction<Tuple2<Integer,String>, Integer, String>() {
                    public Tuple2<Integer,String> call(Tuple2<Integer,String> x) 
                    {    
                        String y = val;
                        Tuple2<Integer,String> xn = new Tuple2<>(x._1,"");
                        int a = (x._2).compareTo(y);
                        switch(op){
                            case 0 :   
                        if (a==0) return xn;
                        else return null;
                            case 1 : 
                        if(a < 0)return xn;
                        else return null;
                            case  2 : 
                        if(a <= 0)return xn;
                        else return null;
                            case 3 :
                        if(a > 0)return xn;
                        else return null;
                            case 4 :
                        if(a >= 0)return xn;
                        else return null;
                            default:
                                    return null;
                        }
                    }
                });      
            }
        toR = alf.filter(x -> {
            return x!= null;
        });  
        return toR;
    }
    
    private JavaPairRDD performOperationWithIndex(int col, Integer op, String val) {
        JavaPairRDD column = this.indexedCol;
        JavaPairRDD toR = null;
        JavaPairRDD alf = null;
        if(!dictionary.containsKey(val))
            return null;
        alf = column.mapToPair(new PairFunction<Tuple2<Integer,Integer>, Integer, String>() {
                public Tuple2<Integer,String> call(Tuple2<Integer,Integer> x) 
                {    
                    Integer y = (int) (long)dictionary.get(val);
                    int x1 = x._2;
                    Tuple2<Integer,String> xn = new Tuple2<>(x._1,"");
                    switch(op){
                        case 0 :   
                            if (x1 == y){
                                return xn;
                            }
                            else return null;
                        /*case 1 : 
                            if (x1 < y)
                                return xn;
                            else return null;
                        case  2 : 
                            if(x1 <= y)
                                return xn;
                            else return null;
                        case 3 :
                            if(x1 > y)
                                return xn;
                            else return null;
                        case 4 :
                            if(x1>= y)
                                return xn;
                            else return null;*/
                        default:
                                return null;
                    
                }
             
            }
        });
        toR = alf.filter(x -> {
            return x!= null;
        }); 
        return toR;
    }
    
    public void applyDictionaryCompression(String column)
    {
        int col = this.findCorrespondingCol(column);
        if(this.getAttr_types()[col].compareTo("String") != 0)
        {
            this.isDictionaryEncoded = false;
            return;
        }
        this.colEncoded = col;
        JavaRDD dist = this.getSpecificRDD(col).values().distinct();
        this.dictionary = dist.zipWithUniqueId().collectAsMap();
        this.indexedCol = this.getSpecificRDD(col).mapToPair(new PairFunction<Tuple2<Integer,String>,Integer,Integer>() {
            @Override
            public Tuple2<Integer,Integer> call(Tuple2<Integer,String> x) throws Exception {
                Integer intx =(int) (long)dictionary.get(x._2);
                return  new Tuple2(x._1,intx);//Tuple2(x._1,(Integer)dictionary.get(x._2));
            }
        });    
    }
    
    public JavaRDD<String> projectResults(List<Integer> projectCols, JavaPairRDD ids)
    {
        List a = new ArrayList<>();
        JavaPairRDD tf = ids;
        for(Integer temp : projectCols)
        {
            tf = tf.join(this.getSpecificRDD(temp)).sortByKey().mapValues(new Function<Tuple2<String,String>,String>() {
                @Override
                public String call(Tuple2<String,String> x) throws Exception {
                    if(x._1.compareTo("")==0)
                        return x._2;
                    else
                        return x._1+","+x._2;
                }
            });
        }
        return tf.values();
    }
    
    public void createOneFile(String outputFile)
    {
        FileFilter urlFilter = new FileFilter() {
               @Override
               public boolean accept(File file) {
                   if (file.isDirectory()) {
                       return true; // return directories for recursion
                   }
                   return file.getName().startsWith("part-00"); // return .url files
               };
               };
          File dir = new File("task3File");
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

    public static void main(String[] args) throws FileNotFoundException, IOException
    {
        if(args.length != 6)
        {
            System.out.println("You should provide 6 parameters!");
            return;
        }    
        String master = "local[4]";
        SparkConf conf = new SparkConf()
            .setAppName(CompressedColumnStore.class.getName())
            .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputFile = args[0];
        String outputFile = args[1];
        String schema = args[2];
        String projectionList = args[3];
        String whereList = args[4];
        String compressedCol = args[5];
        
        BufferedReader br = new BufferedReader(new FileReader(schema));    // read schema
        schema = br.readLine();
        String[] temp = schema.split(",");
        CompressedColumnStore columnstore = new CompressedColumnStore(temp);    // in constructor load schema details into class variables
        JavaRDD<String> lines = sc.textFile(inputFile);
        Set<Integer> columnsToLoad = new HashSet<>();
        List<Integer> columnsToProject = new ArrayList<>();
        String[] wheres = null;
        String[] projections = null;
        //find which columns to Load!
        if(projectionList != null && whereList != null)
        {
            projections = projectionList.split(",");
            wheres = whereList.split(",");
            for(String p : projections)
            {
                int col = columnstore.findCorrespondingCol(p);
                if(!columnsToProject.contains(col))
                    columnsToProject.add(col);
            }
            for (String where : wheres) {
                String[] selection = where.split("\\|");
                int col = columnstore.findCorrespondingCol(selection[0]);
                if(!columnsToLoad.contains(col))
                    columnsToLoad.add(col);
                if(compressedCol != null)
                    if(selection[0].compareTo(compressedCol)==0 && selection[1].compareTo("=") == 0)
                        columnstore.isDictionaryEncoded = true;
            }
            columnsToLoad.addAll(columnsToProject);
        }
        // load only required attributes
        for(int i = 0;i<temp.length;i++)
        {
            JavaRDD tempRDD = null;
            int k = i;
            if(columnsToLoad.contains(k))
                tempRDD = lines.map(e->e.replace("\"", "").split(",")[k]);
            if(tempRDD != null){
                columnstore.setSpecificRDD(tempRDD.zipWithIndex(), k);
            }
        }
        if(columnstore.isDictionaryEncoded)
            columnstore.applyDictionaryCompression(compressedCol);

        JavaPairRDD last = null;
        JavaPairRDD r = null;
        JavaPairRDD row_ids = null;
        for (int kl = 0;kl<wheres.length;kl++)        // executes where clause
        {
            String where = wheres[kl];
            String[] selection = where.split("\\|");    
            r = columnstore.performSelection(selection);
            if(r == null){
                row_ids = null;
                break;
            }
            if(kl != 0)
                row_ids = r.intersection(last);
            if(kl == 0)
                row_ids = r;
            last = r;
        }
        JavaRDD<String> result = null;
        if(row_ids != null){
            result = columnstore.projectResults(columnsToProject, row_ids);
            // returns result
            result.saveAsTextFile("task3File");                 
            columnstore.createOneFile(outputFile);
        }
        else // ERROR HANDLING IN case a where clause returns nothing 
        {
           FileOutputStream outFile = new FileOutputStream(outputFile);
           outFile.close();
        }
        
    }
}
