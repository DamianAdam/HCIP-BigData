package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClientTools {
    public static FileSystem getClient() {
        Configuration configuration = new Configuration();
        String username = "root";   // 使用Hadoop的Linux用户
        URI uri = null;             // NameNode的通信地址，写在core-site.xml中
        FileSystem fs = null;

        try {
            uri = new URI("hdfs://master:9000");
            fs = FileSystem.get(uri, configuration, username);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return fs;
    }

    public static void releaseClient(FileSystem clent) {
        try {
            if (clent != null) clent.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
