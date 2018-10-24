package com.mmall.concurrency.example.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/**
 * @Author: csk
 * @Date: 2018/8/28 14:19
 * zookeeper获取节点创建监听事件
 */
@Slf4j
public class ZookeeperDemo6 implements Watcher {

    private static String zooPath = "119.29.196.106:2181,119.29.196.106:2182,119.29.196.106:2183";

    private static int timeOut = 5000;

    private static Stat stat = new Stat();
    private static CountDownLatch countDownLatch = new CountDownLatch(1);



    private ZooKeeper zooKeeper = null;

    public ZookeeperDemo6(String path) {
        try {
            zooKeeper = new ZooKeeper(path, timeOut, new ZookeeperDemo6());
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

    public ZookeeperDemo6() {
    }

    /**
     * @param path
     */
    public void getNode(String path) {
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
             */
            byte[] data = zooKeeper.getData(path, true, stat);
            String s = new String(data);
            System.out.println(s);
            countDownLatch.await();
//            /**
//             * 异步方法
//             */
//            String ctx = "{'delete',success'}";
//            zooKeeper.delete(path, -1, new MyVoidCallback(), ctx);
//            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        ZookeeperDemo6 zookeeperDemo3 = new ZookeeperDemo6(zooPath);
        zookeeperDemo3.getNode("/helloworld");
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == Event.EventType.None){

        }else if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){

        }else if(watchedEvent.getType() == Event.EventType.NodeCreated){

        }else if(watchedEvent.getType() == Event.EventType.NodeDataChanged){
            ZookeeperDemo6 zookeeperDemo3 = new ZookeeperDemo6(zooPath);
            try {
                byte[] data = zookeeperDemo3.zooKeeper.getData("/helloworld", false, stat);
                System.out.println("变化后的值:"+new String(data));
                System.out.println("变化后的版本:"+stat.getVersion());
                countDownLatch.countDown();
            } catch (Exception e){
                e.printStackTrace();
            }
        }else if(watchedEvent.getType() == Event.EventType.NodeDeleted){

        }

        log.info("接收到watcher信息:{}", watchedEvent);
    }
}
