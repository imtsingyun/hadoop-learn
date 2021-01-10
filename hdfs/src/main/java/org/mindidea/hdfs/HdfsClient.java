/*
 * @class HdfsClient
 * @package org.mindidea.hdfs
 * @date 2020/12/22 16:23
 * Copyright (c) 2020 MindIdea.org, All Rights Reserved.
 */
package org.mindidea.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Tsingyun(青雲)
 * @version V1.0
 * @createTime 2020/12/22 16:23
 * @blog https://mindidea.org
 */
public class HdfsClient {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
        conf.set("user", "root");
        conf.set("password", "666666");

        FileSystem fs = FileSystem.get(conf);

        fs.mkdirs(new Path("/2020/12/22"));

        fs.close();
        System.out.println("over");
    }

    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
        fs.copyFromLocalFile(new Path("E:\\Github\\hadoop\\temp\\a.txt"), new Path("/2020/12/22/aaa.txt"));
        fs.close();
    }

    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
//        fs.copyToLocalFile(new Path("/2020/12/22/aaa.txt"), new Path("E:\\Github\\hadoop\\temp\\aaa.txt"));
        fs.copyToLocalFile(false,
                new Path("/2020/12/22/aaa.txt"),
                new Path("E:\\Github\\hadoop\\temp\\aaa.txt"),
                true);
        fs.close();
    }

    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
        fs.delete(new Path("/2020/12/22/aaa.txt"), true);
        fs.close();
    }

    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
        fs.rename(new Path("/2020/12/22/aa.txt"), new Path("/2020/12/22/b.txt"));
        fs.close();
    }

    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
        RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(new Path("/2020/12/22/"), true);
        while (fileList.hasNext()) {
            LocatedFileStatus next = fileList.next();
            System.out.println(next);
        }
        fs.close();
    }
}
