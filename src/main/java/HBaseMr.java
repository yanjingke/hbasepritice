import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseMr {
    public static final String  tableName="word";
    public static  final String colf="content";//列族
    public static  final String col="info";
    public static final String tableName2="stat";
   public  static Connection connection=null;
    static Configuration config=null;
    static {
        config=HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","hadoop");//zookeeper地址
        config.set("hbase.zookeeper.property.clientPort","2181");//zookeeper端口
        try {
            connection=ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public  static class  HBaseMrMap extends TableMapper<Text,IntWritable>{
        private static IntWritable one=new IntWritable(1);
        private static Text word=new Text();
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String words=Bytes.toString(value.getValue(Bytes.toBytes(colf),Bytes.toBytes(col)));
            String []itr=words.toString().split(" ");
            for (int i=0;i<itr.length;i++){
                word.set(itr[i]);
                context.write(word,one);
            }
        }
    }
    public  static class  HBaseMrReduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable val:values){
                sum+=val.get();
            }
            //设置单词为key
            Put put=new Put(Bytes.toBytes(key.toString()));
            //String.valueOf转出字符串
            put.add(Bytes.toBytes(colf),Bytes.toBytes(col),Bytes.toBytes(String.valueOf(sum)));
            //写入hbase指定的rowkey,put
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);

        }
    }

    public static void initTB() throws IOException {
        HTable table=null;
        HBaseAdmin admin=null;
        admin=new HBaseAdmin(config);
        if(admin.tableExists(tableName)||admin.tableExists(tableName2)){
            System.out.println("table is already exists");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            admin.disableTable(tableName2);
            admin.deleteTable(tableName2);
        }
        HTableDescriptor desc=new HTableDescriptor(tableName);
        HColumnDescriptor infor1 = new HColumnDescriptor(colf);
        desc.addFamily(infor1);
        admin.createTable(desc);
        HTableDescriptor desc2=new HTableDescriptor(tableName2);
        HColumnDescriptor infor2 = new HColumnDescriptor(colf);
        desc2.addFamily(infor2);
        admin.createTable(desc2);
        table=new HTable(config,tableName);
        table.setAutoFlush(false);
        table.setWriteBufferSize(500);
        List<Put>lp=new ArrayList<Put>();
        Put p1=new Put(Bytes.toBytes("1"));
        p1.add(colf.getBytes(),col.getBytes(),("I love you ").getBytes());
        lp.add(p1);
        Put p2=new Put(Bytes.toBytes("2"));
        p2.add(colf.getBytes(),col.getBytes(),("sb xxx3 ").getBytes());
        lp.add(p2);
        Put p3=new Put(Bytes.toBytes("3"));
        p3.add(colf.getBytes(),col.getBytes(),("sb3 xxx2 ").getBytes());
        lp.add(p3);
        Put p4=new Put(Bytes.toBytes("4"));
        p4.add(colf.getBytes(),col.getBytes(),("sb4 xxx1 ").getBytes());
        lp.add(p4);
        table.put(lp);
        table.flushCommits();
        lp.clear();
    }

    public static void main(String[] args) throws Exception {
       // config.set("fs.defaultFS","hdfs://hadoop:9000/");
        initTB();
        Job job = new Job(config, "HBaseMr");
        job.setJarByClass(HBaseMr.class);
        Scan scan=new Scan();
        scan.addColumn(Bytes.toBytes(colf),Bytes.toBytes(col));
        //创建查询的tableName表,设置表名,scan,mapper,输出Key,value
        TableMapReduceUtil.initTableMapperJob(tableName, scan, HBaseMrMap.class,Text.class, IntWritable.class, job);
        //创建输出的tableName2表,设置输出表名,scan,Reduce
        TableMapReduceUtil.initTableReducerJob(tableName2,HBaseMrReduce.class,job);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
