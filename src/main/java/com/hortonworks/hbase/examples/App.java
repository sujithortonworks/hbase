package com.hortonworks.hbase.examples;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import java.util.NoSuchElementException;
import java.io.FileNotFoundException;
import org.apache.hadoop.hbase.security.User;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.hbase.security.UserProvider;


public class App
{
  private static final byte[] CF = Bytes.toBytes("f1");
private static TableName tn1 = TableName.valueOf("clay_impersonation_test");

    public static void main( String[] args )  throws Exception
    {
//      HBaseConfiguration conf = new HBaseConfiguration();
  //  final Connection conn = ConnectionFactory.createConnection(conf);

    final UserGroupInformation proxy =
      UserGroupInformation.createProxyUser("ambari-qa",
      UserGroupInformation.getLoginUser());
System.out.println(UserGroupInformation.getLoginUser());

    proxy.doAs(
      new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {

//HDFS
         final  Configuration hdfsConfiguration = new Configuration();
/*
FileSystem fs = FileSystem.get(hdfsConfiguration);
FileStatus[] fsStatus = fs.listStatus(new Path("/"));

for(int i = 0; i <= fsStatus.length; i++){
  System.out.println(fsStatus[i].getPath().toString());
}
*/
//HBASE
        HBaseConfiguration conf = new HBaseConfiguration();
        final UserProvider userProvider = new UserProvider();

        final Connection conn = ConnectionFactory.createConnection(conf , userProvider.create(proxy));
        Admin admin = conn.getAdmin();
          HTableDescriptor htd = new HTableDescriptor(tn1);

        HColumnDescriptor family = new HColumnDescriptor(CF);
        htd.addFamily(family);
        htd.setConfiguration("hbase.table.sanity.checks", "false");

        admin.createTable(htd);
        System.out.println("Table created");

        admin.disableTable(tn1);
        admin.deleteTable(tn1);
        System.out.println("Table deleted");

          return null;
        }
      });
}}
