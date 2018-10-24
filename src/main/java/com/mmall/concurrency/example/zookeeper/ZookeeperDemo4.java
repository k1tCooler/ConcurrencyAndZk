package com.mmall.concurrency.example.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @Author: csk
 * @Date: 2018/8/28 14:19
 * zookeeper修改节点
 */
@Slf4j
public class ZookeeperDemo4 implements Watcher {

    private static String zooPath = "119.29.196.106:2181,119.29.196.106:2182,119.29.196.106:2183";

    private static int timeOut = 5000;

    private ZooKeeper zooKeeper = null;

    public ZookeeperDemo4(String path) {
        try {
            zooKeeper = new ZooKeeper(path, timeOut, new ZookeeperDemo4());
        } catch (Exception e) {
            e.printStackTrace();
            if (zooKeeper != null) {
                try {
                    zooKeeper.close();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public ZookeeperDemo4() {
    }

    /**
     * @param path
     * @param data
     */
    public void updateNode(String path, byte[] data) {
        String result = "";
        try {
            /**
             * 同步或者异步创建节点，都不支持子节点的递归创建，异步有一个callback函数
             * 参数：
             * path：创建的路径
             * data：存储数据的byte[]
             * acl：控制权限策略
             *      Ids.OPEN_ACL_UNSAFE --> world:anyone:cdrwa
             *      CREATE_ALL_ACL --> auth:user:password:cdrwa
             * createMode: 节点类型，是一个枚举类
             *      PERSISTENT: 持久节点
             *      PERSISTENT_SEQUENTIAL: 持久序列节点
             *      EPHEMERAL: 临时节点
             *      EPHEMERAL_SEQUENTIAL: 临时序列节点
             */

            /**
             * 同步方法
             * version 指定版本保证原子性，通过cas    -1 指最新的版本
             */
           /* Stat stat = zooKeeper.setData(path, data, -1);
            System.out.println(stat.getVersion());*/

            /**
             * 异步方法
             */
            String ctx = "{'update',success'}";
            zooKeeper.setData(path, data, -1, new MyStatCallback(), ctx);
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        ZookeeperDemo4 zookeeperDemo3 = new ZookeeperDemo4(zooPath);
        zookeeperDemo3.updateNode("/helloworld", "updatehelloworld".getBytes());
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info("接收到watcher信息:{}", watchedEvent);
    }


    public class MyStatCallback implements AsyncCallback.StatCallback {

        @Override
        public void processResult(int i, String s, Object o, Stat stat) {
            log.info("修改节点{}", s, o);
        }
    }
}
