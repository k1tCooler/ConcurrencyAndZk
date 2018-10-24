package com.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class CuratorOperator {

    public CuratorFramework client = null;
    public static final String zkServerPath = "119.29.196.106:2181,119.29.196.106:2182,119.29.196.106:2183";

    /**
     * 实例化zk客户端
     */
    public CuratorOperator() {
        /**
         * 同步创建zk示例，原生api是异步的
         *
         * curator链接zookeeper的策略:ExponentialBackoffRetry
         * baseSleepTimeMs：初始sleep的时间
         * maxRetries：最大重试次数
         * maxSleepMs：最大重试时间
         */
//		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

        /**
         * curator链接zookeeper的策略:RetryNTimes
         * n：重试的次数
         * sleepMsBetweenRetries：每次重试间隔的时间
         */
        RetryPolicy retryPolicy = new RetryNTimes(3, 5000);

        /**
         * curator链接zookeeper的策略:RetryOneTime
         * sleepMsBetweenRetry:每次重试间隔的时间
         */
//		RetryPolicy retryPolicy2 = new RetryOneTime(3000);

        /**
         * 永远重试，不推荐使用
         */
//		RetryPolicy retryPolicy3 = new RetryForever(retryIntervalMs)

        /**
         * curator链接zookeeper的策略:RetryUntilElapsed
         * maxElapsedTimeMs:最大重试时间
         * sleepMsBetweenRetries:每次重试间隔
         * 重试时间超过maxElapsedTimeMs后，就不再重试
         */
//		RetryPolicy retryPolicy4 = new RetryUntilElapsed(2000, 3000);

        client = CuratorFrameworkFactory.builder()
                .connectString(zkServerPath)
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy)
                .namespace("workspace").build();
        client.start();
    }

    /**
     * @Description: 关闭zk客户端连接
     */
    public void closeZKClient() {
        if (client != null) {
            this.client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        // 实例化
        CuratorOperator cto = new CuratorOperator();
        boolean isZkCuratorStarted = cto.client.isStarted();
        System.out.println("当前客户的状态：" + (isZkCuratorStarted ? "连接中" : "已关闭"));

        // 创建节点
        String nodePath = "/imooc/curator";
//		byte[] bytes = "superme".getBytes();
//		cto.client.create().creatingParentsIfNeeded()
//				.withMode(CreateMode.PERSISTENT)
//				.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
//				.forPath(nodePath,bytes);

        //删除节点
//        cto.client.delete()
//                .guaranteed() //网络抖动依然保证删除
//                .deletingChildrenIfNeeded()//子节点照样删除
//                .withVersion(-1)
//                .forPath(nodePath);

        //查询节点
//		Stat stat = new Stat();
//		byte[] bytes = cto.client.getData().storingStatIn(stat).forPath(nodePath);
//		System.out.println("数据："+new String(bytes));
//		System.out.println("节点版本："+ stat.getVersion());

        //查询子节点
//		List<String> childrens = cto.client.getChildren().forPath(nodePath);
//		for (String children : childrens) {
//			System.out.println(children);
//		}

        //查询是否存在节点
//        Stat statExit = cto.client.checkExists().forPath(nodePath);
//        System.out.println(statExit);/*null为不存在*/


        //只会监听一次，并不会重复监听事件
//        cto.client.getData().usingWatcher(new MyCuratorWatch()).forPath(nodePath);

        //创建节点缓存 创建监听事件 可以监听N次
//        NodeCache nodeCache = new NodeCache(cto.client, nodePath);
//        nodeCache.start();
//        if(nodeCache.getCurrentData()!=null){
//            System.out.println(new String(nodeCache.getCurrentData().getData()));
//        }else{
//            System.out.println("节点没有数据");
//        }
//        nodeCache.getListenable().addListener(()->{
//            if(nodeCache.getCurrentData()==null){
//                System.out.println("节点没有数据,已经删除");
//                return;
//            }
//            System.out.println(new String(nodeCache.getCurrentData().getData()));
//            System.out.println(nodeCache.getCurrentData().getPath());
//        });

        //创建父节点，监听所有的子节点
        PathChildrenCache pathChildrenCache = new PathChildrenCache(cto.client, nodePath, true);

        /**
         * StartMode:初始化方式
         *  POST_INITIALIZED_EVENT :异步初始化，初始化后会触发事件，可以通过监听器监听
         *  NORMAL :异步初始化
         *  BUILD_INITIAL_CACHE :同步初始化 pathChildrenCache.getCurrentData()会有返回值
         * */
        pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
       /* List<ChildData> currentData = pathChildrenCache.getCurrentData();
        for (ChildData currentDatum : currentData) {
            System.out.println(currentDatum.getData());
        }*/
        pathChildrenCache.getListenable().addListener((client,event)->{
            if(event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)){
                System.out.println("子节点初始化OK");
            }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)){
                String path = event.getData().getPath();
                if(path.equals("/aaa/bbb")){
                    System.out.println("子节点新增业务处理bbb节点");
                }else if(path.equals("/aaa/ccc")){
                    System.out.println("子节点新增业务处理cc节点");
                }
            }
        });

        Thread.sleep(3000);
        cto.closeZKClient();
        boolean isZkCuratorStarted2 = cto.client.isStarted();
        System.out.println("当前客户的状态：" + (isZkCuratorStarted2 ? "连接中" : "已关闭"));
    }


}
