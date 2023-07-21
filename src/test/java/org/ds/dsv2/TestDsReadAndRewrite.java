package org.ds.dsv2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.ds.dsv2.Utils.*;
import static org.junit.Assert.assertEquals;
import static org.lmdbjava.Env.create;


public class TestDsReadAndRewrite {

    @Rule
    public TemporaryFolder tmp= new TemporaryFolder();

    File tmpLMDBpath;

    @After
    public void after() {

    }

    @Before
    public void before() throws IOException {
        tmpLMDBpath = tmp.newFolder();
        Env<ByteBuffer> createEnv = create()
                .setMapSize(10 * 1024 * 1024) //10MB
                .setMaxReaders(2)
                .setMaxDbs(2)
                .open(tmpLMDBpath);
        try {
            final Dbi<ByteBuffer> db = createEnv.openDbi((String) null, null);
            try (Txn<ByteBuffer> txn = createEnv.txnWrite()) {
                int count;
                for (count = 0; count < 10; count++) {
                    String k = String.format("%1$10s", count + "").replace(' ', '0');
                    //String v = k + "-value";
                    db.put(txn, str_to_bb(k), int_to_bb(count));
                }
                //db.put(txn, str_to_bb("num-samples"), str_to_bb(count + ""));
                txn.commit();
            }
        }finally {
            createEnv.close();
        }
    }

    @Test
    public void testDsReadAndRewrite() throws IOException {
        String dataSourceName    = "org.ds.dsv2";

        try (SparkSession spark = SparkSession
                .builder()
                .appName("testRead")
                .master("local[2]")
                .getOrCreate()) {
            Dataset<Row> df = spark.read()
                    .format(dataSourceName)
                    .option(Constants.LMDB_IN_DIR, tmpLMDBpath.getPath())
                    .load();

            System.out.println("*** Schema: ");
            df.printSchema();

            df = df.cache();

            df.select(sum(col("value"))).show();
            Object value = ((Row[]) df.select(sum(col("value"))).collect())[0].get(0);
            assertEquals( 45L, value);

            long lenOneParam =  df.filter(col("key").equalTo("0000000001"))
                    .count();
            assertEquals( 1, lenOneParam);

            File outTestDir = tmp.newFolder();
            df.write().format(dataSourceName).mode(SaveMode.Append).save(outTestDir.getPath());

            Dataset<Row> df2 = spark.read()
                    .format(dataSourceName)
                    .option(Constants.LMDB_IN_DIR, outTestDir.getPath())
                    .load();

            df2 = df2.cache();
            df2.select(sum(col("value"))).show();
            Object values2 = ((Row[]) df2.select(sum(col("value"))).collect())[0].get(0);
            assertEquals( 45L, values2);

            long lenOneParam2 =  df2.filter(col("key").equalTo("0000000001"))
                    .count();
            assertEquals( 1, lenOneParam2);
        }
    }

}
