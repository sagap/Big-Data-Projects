import java.util.*;

public class Skyline {
    public static ArrayList<Tuple> mergePartitions(ArrayList<ArrayList<Tuple>> partitions){
        Iterator<ArrayList<Tuple>> it = partitions.iterator();
        ArrayList<Tuple> temp= new ArrayList<>();
        while(it.hasNext())
        {
            temp.addAll(it.next());
        }
        return temp;
    }

    public static ArrayList<Tuple> dcSkyline(ArrayList<Tuple> inputList, int blockSize){
        ArrayList<ArrayList<Tuple>> partitions = new ArrayList<>();
        ArrayList<Tuple> temp = new ArrayList<>(blockSize);
        int counter = 0;
        // Partition Data compute skylines for each small partition
        for(int i=0;i<inputList.size();i++)
        {
            counter++;
            temp.add(inputList.get(i));
            if(counter % blockSize ==0)
            {
                partitions.add(nlSkyline(temp));
                temp = new ArrayList<>(5);
            }
        }
        if(!temp.isEmpty()){
            partitions.add(nlSkyline(temp));
        }
        int k= partitions.size();
        // union by taking pairs and compute skyline of each union
	// continue merging until partitions are only 2 or 1
        while(partitions.size()>2)
        {
            k = partitions.size();
            for(int i = 0;i<Math.ceil((double)partitions.size()/(double)2);i++)
            {
                ArrayList<ArrayList<Tuple>> tempL;
                if(i*2 >= k)
                    continue;
                if(i*2+1 == k){
                    tempL = new ArrayList<>(partitions.subList(i*2, i*2+1));
                }
                else
                {
                    tempL = new ArrayList<>(partitions.subList(i*2, i*2+2));
                }
                partitions.set(i,nlSkyline(mergePartitions(tempL)));     
            }
                 partitions.subList((int)Math.ceil((double)partitions.size()/(double)2), partitions.size()).clear();
        }
        if(partitions.size()==2)
        {
            return nlSkyline(mergePartitions(new ArrayList<>(partitions.subList(0,2))));
        }
        else{
            return nlSkyline(mergePartitions(new ArrayList<>(partitions.subList(0,1))));
        }
    }

        public static ArrayList<Tuple> nlSkyline(ArrayList<Tuple> partition) {        
            Tuple candidate,temp;
            boolean flag;
            int a = partition.size();
            for(int i =0;i<a;i++)     // outer iteration for candidates
            {
                flag = true;
                candidate = partition.get(i);     // candidate
                for(int j = i+1;j<a;j++)
                {
                    temp = partition.get(j);      
                    if(i != j)
                    {
                        if(candidate.dominates(temp))
                        {
                            partition.remove(j);
                            j--;
                            a = partition.size();               
                        }
                        else if(candidate.isIncomparable(temp))   
                        {
                            continue;
                        }
                        else    // candidate is dominated
                        {
                            flag = false;
                            break;
                        }    
                    }
                }
                if(!flag)      // remove candidate
                {
                    partition.remove(i);
                    i = (i == 0?-1:i-1);
                    a = partition.size();
                }
            }
            return partition;
        }
}

class Tuple {
    private int price;
    private int age;

    public Tuple(int price, int age){
        this.price = price;
        this.age = age;
    }

    public boolean dominates(Tuple other){
        if(this.price < other.price)
            if(this.age <= other.age)
                return true;
        if(this.age < other.age)
            if(this.price <= other.price)
                return true;
        return false;
    
    }

    public boolean isIncomparable(Tuple other){
        return !(this.dominates(other) || other.dominates(this));
    }

    public int getPrice() {
        return price;
    }

    public int getAge() {
        return age;
    }

    public String toString(){
        return price + "," + age;
    }

    public boolean equals(Object o) {
        if(o instanceof Tuple) {
            Tuple t = (Tuple)o;
            return this.price == t.price && this.age == t.age;
        } else {
            return false;
        }
    }
}    
