package com.database;

import com.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HBase {

    private TableName normalizedOrder = TableName.valueOf(Constants.HTABLE_NAME);


    public Configuration getHBaseConfig() {
        Configuration config = HBaseConfiguration.create();
        String configPath = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();

        config.addResource(new Path(configPath));
        return config;
    }

    public Connection getHBaseConnection() throws IOException {
        Configuration config = getHBaseConfig();
        return ConnectionFactory.createConnection(config);
    }


    public void createOrderTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor desc = getTableDescriptor();
        admin.createTable(desc);
        admin.close();
    }

    public boolean isTableExist(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        TableName[] tableNames = admin.listTableNames(Constants.HTABLE_NAME);
        return tableNames.length > 0;
    }

    public HTableDescriptor[] listTables(Connection connection) throws IOException {

        Admin admin=connection.getAdmin();
        return admin.listTables(Constants.HTABLE_NAME);
    }

    private HTableDescriptor getTableDescriptor() {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(normalizedOrder);

        HColumnDescriptor basic_info = new HColumnDescriptor("basic_info");
        hTableDescriptor.addFamily(basic_info);

        HColumnDescriptor tags = new HColumnDescriptor("tags");
        hTableDescriptor.addFamily(tags);

        return hTableDescriptor;
    }

}
