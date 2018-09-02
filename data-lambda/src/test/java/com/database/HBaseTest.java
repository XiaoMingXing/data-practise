package com.database;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HBaseTest {

    private HBase hbase = new HBase();

    @Test
    public void isHBaseAvailable() throws IOException, ServiceException {
        HBaseAdmin.checkHBaseAvailable(this.hbase.getHBaseConfig());
    }

    @Test
    public void shouldNotHaveTable() throws IOException {

        boolean tableExist = hbase.isTableExist(hbase.getHBaseConnection());
        assertFalse(tableExist);
    }

    @Test
    public void listAllTheTables() throws IOException {
        HTableDescriptor[] hTableDescriptors = hbase.listTables(this.hbase.getHBaseConnection());
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getNameAsString());
        }
    }
}