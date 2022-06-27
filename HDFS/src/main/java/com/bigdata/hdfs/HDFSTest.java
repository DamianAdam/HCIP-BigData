package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HDFSTest {
    @Test
    public void testMkdir() throws IOException {
        // 获取HDFS对象。
        FileSystem client = HdfsClientTools.getClient();

        // 调用HDFS对象的mkdirs()方法，将目录路径传入。
        Path dirPath = new Path("/xiyou/huaguoshan");
        client.mkdirs(dirPath);

        // 关闭对象。
        HdfsClientTools.releaseClient(client);
    }

    @Test
    public void testPut() throws IOException {
        // 获取HDFS对象。
        FileSystem client = HdfsClientTools.getClient();

        // 上传文件
        Path src = new Path("D:/Hello.txt");
        Path dst = new Path("/xiyou");
        client.copyFromLocalFile(false, true, src, dst);

        // 关闭对象。
        HdfsClientTools.releaseClient(client);
    }

    @Test
    public void testPut1() throws IOException, URISyntaxException, InterruptedException {
        // 通过Configuration对象设置副本数为2。
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");

        // 获取HDFS对象。
        FileSystem client = FileSystem.get(
                new URI("hdfs://master:9000"),
                configuration,
                "root"
        );

        // 上传文件
        Path src = new Path("D:/Hello.txt");
        Path dst = new Path("/xiyou/Hello2.txt");
        client.copyFromLocalFile(false, true, src, dst);

        // 关闭对象。
        HdfsClientTools.releaseClient(client);
    }

    @Test
    public void testDownloadFile() throws IOException {
        // 获取HDFS对象。
        FileSystem client = HdfsClientTools.getClient();

        // 下载文件。
        Path src = new Path("/xiyou/Hello.txt");
        Path dst = new Path("D:/Hello1.txt");
        client.copyToLocalFile(false, src, dst, true);

        // 关闭对象。
        HdfsClientTools.releaseClient(client);
    }

    @Test
    public void testRemoveFile() throws IOException {
        // 获取HDFS对象。
        FileSystem client = HdfsClientTools.getClient();

        // 删除文件和空目录。
        client.delete(new Path("/xiyou/huaguoshan/Hello.txt"), false);
        client.delete(new Path("/xiyou/shuiliandong"), false);

        // 递归删除非空目录。
        client.delete(new Path("/xiyou"), true);

        // 关闭对象。
        HdfsClientTools.releaseClient(client);
    }

    @Test
    public void testMoveAndRename() throws IOException {
        // 获取HDFS对象。
        FileSystem client = HdfsClientTools.getClient();

        // 移动并更名文件。
        client.rename(new Path("/input/word.txt"), new Path("/input_words.txt"));

        // 更名目录。
        client.rename(new Path("/input"), new Path("/output"));

        // 关闭对象。
        HdfsClientTools.releaseClient(client);
    }

    @Test
    public void fileDetails() throws IOException {
        // 获取HDFS对象。
        FileSystem client = HdfsClientTools.getClient();

        // 获取指定路径下所有文件的信息。(一个迭代器)
        RemoteIterator<LocatedFileStatus> listFiles = client.listFiles(new Path("/"), true);

        // 遍历迭代器中存储着的所有文件.
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            // 获取文件的信息。
            System.out.println("======" + fileStatus.getPath() + "======");     // 打印文件的路径
            System.out.println(fileStatus.getPermission());                     // 打印文件的权限
            System.out.println(fileStatus.getOwner());                          // 打印文件的所有者
            System.out.println(fileStatus.getGroup());                          // 打印文件的所属组
            System.out.println(fileStatus.getLen());                            // 打印文件的长度（即大小）
            System.out.println(fileStatus.getModificationTime());               // 打印文件最后一次修改的时间（时间戳）
            System.out.println(fileStatus.getReplication());                    // 打印文件的副本数
            System.out.println(fileStatus.getBlockSize());                      // 打印文件的块大小
            System.out.println(fileStatus.getPath().getName());                 // 打印文件名

            // 获取文件块存储的位置
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }

        // 关闭对象。
        HdfsClientTools.releaseClient(client);
    }

    @Test
    public void testFileOrDir() throws IOException {
        // 获取HDFS对象。
        FileSystem client = HdfsClientTools.getClient();

        // 遍历指定目录。
        FileStatus[] statuses = client.listStatus(new Path("/"));

        // 查看目录类的内容是文件还是目录。
        for (FileStatus status : statuses) {
            if (status.isFile()) System.out.println("文件：" + status.getPath().getName());
            if (status.isDirectory()) System.out.println("目录：" + status.getPath().getName());
        }

        // 关闭对象。
        HdfsClientTools.releaseClient(client);
    }
}
