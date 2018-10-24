package com.mmall.concurrency.example.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.util.List;

/**
 * @Author: csk
 * @Date: 2018/8/28 14:19
 * zookeeper创建节点
 */
@Slf4j
public class ZookeeperDemo3 implements Watcher {

    private static String zooPath = "119.29.196.106:2181,119.29.196.106:2182,119.29.196.106:2183";

    private static int timeOut = 5000;

    private ZooKeeper zooKeeper = null;

    public ZookeeperDemo3(String path) {
        try {
            zooKeeper = new ZooKeeper(path, timeOut, new ZookeeperDemo3());
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

    public ZookeeperDemo3() {
    }

    /**
     * @param path
     * @param data
     * @param acls
     */
    public void createNode(String path, byte[] data, List<ACL> acls) {
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

//            /**
//             * 同步方法
//             */
//            result = zooKeeper.create(path, data, acls, CreateMode.EPHEMERAL);
//            System.out.println(result);
            /**
             * 异步方法
             */
            String ctx="{'create',success'}";
            zooKeeper.create(path, data, acls, CreateMode.PERSISTENT,new MyCallback(),ctx);
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        ZookeeperDemo3 zookeeperDemo3 = new ZookeeperDemo3(zooPath);
        zookeeperDemo3.createNode("/helloworld","helloworld".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info("接收到watcher信息:{}", watchedEvent);
    }


    public class MyCallback implements AsyncCallback.StringCallback{

        @Override
        public void processResult(int i, String s, Object o, String s1) {
            log.info("创建节点{}",s,o);
        }
    }
}
