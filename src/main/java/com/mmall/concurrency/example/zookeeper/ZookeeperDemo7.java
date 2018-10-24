package com.mmall.concurrency.example.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Author: csk
 * @Date: 2018/8/28 14:19
 * zookeeper获取子节点
 */
@Slf4j
public class ZookeeperDemo7 implements Watcher {

    private static String zooPath = "119.29.196.106:2181,119.29.196.106:2182,119.29.196.106:2183";

    private static int timeOut = 5000;

    private ZooKeeper zooKeeper = null;

    private CountDownLatch countDownLatch = new CountDownLatch(1);


    public ZookeeperDemo7(String path) {
        try {
            zooKeeper = new ZooKeeper(path, timeOut, new ZookeeperDemo7());
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

    public ZookeeperDemo7() {
    }

    /**
     * @param path
     */
    public void getChildNode(String path) {
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
            List<String> children = zooKeeper.getChildren(path, true);
            for (String child : children) {
                System.out.println("子节点名称:"+child);
            }

            /**
             * 异步方法
             */
            String ctx = "{'getChildren',success'}";
            zooKeeper.getChildren(path,true,new Children2CallBack(),ctx);

            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        ZookeeperDemo7 zookeeperDemo3 = new ZookeeperDemo7(zooPath);
        zookeeperDemo3.getChildNode("/imooc");
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == Event.EventType.None){

        }else if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged){

        }else if(watchedEvent.getType() == Event.EventType.NodeCreated){

        }else if(watchedEvent.getType() == Event.EventType.NodeDataChanged){
            ZookeeperDemo7 zookeeperDemo7 = new ZookeeperDemo7(zooPath);
            /**
             * 子节点修改值时，并不能触发事件，只有删除新增时才会触发父节点事件
             */
            try {
                List<String> children = zookeeperDemo7.zooKeeper.getChildren(watchedEvent.getPath(), false);
                for (String child : children) {
                    System.out.println("变化后的值:"+child);
                }
                countDownLatch.countDown();
            } catch (Exception e){
                e.printStackTrace();
            }
        }else if(watchedEvent.getType() == Event.EventType.NodeDeleted){

        }

        log.info("接收到watcher信息:{}", watchedEvent);    }


    public class Children2CallBack implements AsyncCallback.Children2Callback {
        @Override
        public void processResult(int i, String s, Object o, List<String> list, Stat stat) {

        }
    }
}
