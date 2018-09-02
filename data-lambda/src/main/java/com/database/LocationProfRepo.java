package com.database;

import com.common.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class LocationProfRepo {

    private Log logger = LogFactory.getLog("locationPreferenceTag");

    private HBase hbase;


    public void initialize() throws IOException {
        synchronized (this) {
            if (hbase == null) {
                hbase = new HBase();
            }
        }
        Connection connection = hbase.getHBaseConnection();
        if (!this.hbase.isTableExist(connection)) {
            logger.info(String.format("[HBase][Initialize] table %s does not exist", Constants.HTABLE_NAME));
            this.hbase.createOrderTable(connection);
        }
    }

}
