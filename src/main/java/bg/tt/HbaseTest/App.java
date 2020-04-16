package bg.tt.HbaseTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class App {
	public static void main(String[] args) throws IOException {
		App a = new App();
		System.out.println("start");
		a.init();
		a.putMutiOk();
		
		a.close();
		System.out.println("end");
	}
	
	Connection conn = null;
	Admin admin = null;
	TableName tn=TableName.valueOf("t1");
	
	
	public void init() {
		try {
		    //1.获得配置文件对象
		    Configuration conf=HBaseConfiguration.create();
		    //设置配置参数
		    conf.set("hbase.zookeeper.quorum", "192.168.175.101:2181");
		    conf.set("hbase.rootdir", "hdfs://192.168.175.101/hbase");
		    
		    //2.建立连接
		    conn=ConnectionFactory.createConnection(conf);
		    //3.获得会话
		    admin=conn.getAdmin();
		    
		    if(admin.tableExists(tn)){
//		        System.out.println("====> 表存在，删除表....");
//		        //先使表设置为不可编辑，关闭表
//		        admin.disableTable(tn);
//		        //删除表
//		        admin.deleteTable(tn);
//		        System.out.println("表删除成功.....");
		    	System.out.println("exist");
		    }
		    else {
		    	System.out.println("not exist");
		    }

		    
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void creatTable(TableName tableName) {
		try {
			if(admin.tableExists(tableName)) {
				return;
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		int PartitionNum = 1000;
		byte splitBytes[][] = new byte[PartitionNum][];
		for(int i = 0; i < PartitionNum; i++) {
			splitBytes[i] = (String.format("%04d", i*10000/PartitionNum)).getBytes();
		}
		
		TableDescriptor desc = TableDescriptorBuilder
				.newBuilder(tableName)
				.setColumnFamily(ColumnFamilyDescriptorBuilder
						.newBuilder("CF".getBytes())
						//.setCompressionType(Algorithm.GZ)
						.build())
				//.setMaxFileSize(1024*1024*1024)
				.build();
		try {
			admin.createTable(desc,splitBytes);		
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		return;
	}
	
	public void putOne() throws IOException {
		Put put =new Put(Bytes.toBytes("row01"));//参数是行健row01
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col1"), Bytes.toBytes("value01"));

		//获得表对象
		Table table=conn.getTable(tn);
		table.put(put);
	}
	
	public void putMuti() throws IOException {
		Put put01 =new Put(Bytes.toBytes("row02"));//参数是行健
		put01.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col2"), Bytes.toBytes("value02"))
		.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col3"), Bytes.toBytes("value03"));

		Put put02 =new Put(Bytes.toBytes("row03"));//参数是行健
		put02.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col4"), Bytes.toBytes("value04"));

		List<Put> puts=Arrays.asList(put01,put02);

		//获得表对象
		Table table=conn.getTable(tn);
		table.put(puts);
	}
	
	public void putMutiOk() throws IOException {	//	批量异步插入方式，效率更高
		
		BufferedMutatorParams mutatorParams = new BufferedMutatorParams(tn)	;
		
		mutatorParams.writeBufferSize(1024*1024);//1Mb
		
//		mutatorParams.rpcTimeout(10);		
//		mutatorParams.setWriteBufferPeriodicFlushTimeoutMs(100000);//设置自动刷新缓冲区之前的最大超时时间		
//		mutatorParams.setWriteBufferPeriodicFlushTimerTickMs(100) ;//如果选中，则设置timer检查缓冲区超时的频率。
		
		BufferedMutator table = conn.getBufferedMutator(mutatorParams);	
		
		
		Put put01 =new Put(Bytes.toBytes("row02"));//参数是行健
		put01.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col2"), Bytes.toBytes("value02"))
		     .addColumn(Bytes.toBytes("info"), Bytes.toBytes("col3"), Bytes.toBytes("value03"));
		
		Put put02 =new Put(Bytes.toBytes("row03"));//参数是行健
		put02.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col4"), Bytes.toBytes("value04"));
		List<Put> puts=Arrays.asList(put01,put02);		
		table.mutate(puts);
		table.flush();
		table.close();		
	}
	
	public void scanTest() throws IOException {
		Scan scan=new Scan().setRowPrefixFilter("123".getBytes());//设置前缀查询
		//获得表对象
		Table table=conn.getTable(tn);
		//得到扫描的结果集		
		ResultScanner rs=table.getScanner(scan);
		for(Result result:rs){
		    //得到单元格集合
		    List<Cell> cs=result.listCells();
		    for(Cell cell:cs){
		        //取行健
		        String rowKey=Bytes.toString(CellUtil.cloneRow(cell));
		        //取到时间戳
		        long timestamp = cell.getTimestamp();
		        //取到族列
		        String family = Bytes.toString(CellUtil.cloneFamily(cell));  
		        //取到修饰名
		        String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell)); 
		        //取到值
		        String value = Bytes.toString(CellUtil.cloneValue(cell));  

		        System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " +
		        timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
		    }
		}

	}
	
	public void getTest() throws IOException {
		Get get = new Get(Bytes.toBytes("row01"));
		get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("col2"));
		Table table = conn.getTable(tn);
		Result r = table.get(get);
		List<Cell> cs = r.listCells();
		for (Cell cell : cs) {
		    String rowKey = Bytes.toString(CellUtil.cloneRow(cell));  //取行键
		    long timestamp = cell.getTimestamp();  //取到时间戳
		    String family = Bytes.toString(CellUtil.cloneFamily(cell));  //取到族列
		    String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));  //取到修饰名
		    String value = Bytes.toString(CellUtil.cloneValue(cell));  //取到值

		    System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " + 
		    timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
		}
	}
		
	public void close() {
		try {
			admin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
