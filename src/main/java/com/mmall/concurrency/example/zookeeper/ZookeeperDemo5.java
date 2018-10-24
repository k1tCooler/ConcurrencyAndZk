package com.mmall.concurrency.example.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @Author: csk
 * @Date: 2018/8/28 14:19
 * zookeeper删除节点
 */
@Slf4j
public class ZookeeperDemo5 implements Watcher {

    private static String zooPath = "119.29.196.106:2181,119.29.196.106:2182,119.29.196.106:2183";

    private static int timeOut = 5000;

    private ZooKeeper zooKeeper = null;

    public ZookeeperDemo5(String path) {
        try {
            zooKeeper = new ZooKeeper(path, timeOut, new ZookeeperDemo5());
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

    public ZookeeperDemo5() {
    }

    /**
     * @param path
     * @param data
     */
    public void deleteNode(String path) {
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
             * 删除建议使用异步方法，能够callback通知
             */
//            zooKeeper.delete(path, -1);

            /**
             * 异步方法
             */
            String ctx = "{'delete',success'}";
            zooKeeper.delete(path, -1, new MyVoidCallback(), ctx);
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        ZookeeperDemo5 zookeeperDemo3 = new ZookeeperDemo5(zooPath);
        zookeeperDemo3.deleteNode("/helloworld");
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info("接收到watcher信息:{}", watchedEvent);
    }


    public class MyVoidCallback implements AsyncCallback.VoidCallback {
        @Override
        public void processResult(int i, String s, Object o) {
            log.info("修改节点{}", s, o);
        }
    }
}
