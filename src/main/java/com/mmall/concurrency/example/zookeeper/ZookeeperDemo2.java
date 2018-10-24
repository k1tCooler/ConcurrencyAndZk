package com.mmall.concurrency.example.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * @Author: csk
 * @Date: 2018/8/28 14:19
 * zookeeper重连
 */
@Slf4j
public class ZookeeperDemo2 implements Watcher {

    private static String zooPath = "119.29.196.106:2181,119.29.196.106:2182,119.29.196.106:2183";

    private static int timeOut = 5000;


    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(zooPath,timeOut,new ZookeeperDemo2());
        long sessionId = zooKeeper.getSessionId();
        String ssid = "0x"+Long.toHexString(sessionId);
        System.out.println(ssid);

        byte[] sessionPasswd = zooKeeper.getSessionPasswd();

        log.info("连接状态:{}",zooKeeper.getState());
        Thread.sleep(2000);
        log.info("连接状态:{}",zooKeeper.getState());

        /**
         * sessionId和sessionPasswd 可存放到redis或者session中
         */
        ZooKeeper reLinkZooKeeper = new ZooKeeper(zooPath,timeOut,new ZookeeperDemo2(),sessionId,sessionPasswd);

        log.info("重连状态:{}",reLinkZooKeeper.getState());
        Thread.sleep(2000);
        log.info("重连状态:{}",reLinkZooKeeper.getState());

    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info("接收到watcher信息:{}",watchedEvent);
    }
}
