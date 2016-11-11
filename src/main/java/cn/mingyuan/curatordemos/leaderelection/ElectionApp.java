package cn.mingyuan.curatordemos.leaderelection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.TimeUnit;

/**
 * 使用curator-framework 实现选举主节点
 *
 * @author jiangmingyuan@myhaowai.com
 * @version 2016/11/11 16:33
 * @since jdk1.8
 */
public class ElectionApp {
    private static final Logger LOG = Logger.getLogger(Logger.class);
    private String zkConnectionString;
    private String electionPath;
    private CuratorFramework curatorFramework;

    public ElectionApp(String zkConnectionString, String electionPath) {
        this.zkConnectionString = zkConnectionString;
        this.electionPath = electionPath;
    }

    /**
     * 初始化curator-framework，并检查选举节点是否存在，若不存在则尝试创建
     *
     * @throws Exception 当发生错误时抛出异常
     */
    public void init() throws Exception {
        curatorFramework = CuratorFrameworkFactory.builder().retryPolicy(new RetryForever(100)).connectString(zkConnectionString).build();
        curatorFramework.start();
        Stat stat = curatorFramework.checkExists().forPath(this.electionPath);
        if (stat == null) {
            LOG.info("election path not exists,try create it");
            String s = curatorFramework.create().creatingParentsIfNeeded().forPath(electionPath);
            if (!s.equals(electionPath)) {
                throw new Exception("create election path exception");
            } else {
                LOG.info("election path created");
            }
        }
        LOG.info("curator framework init ok");
    }

    /**
     * 抢占主节点（take leadership），若成功则启动任务，否则继续等待机会
     */
    public void startJob() {
        //构造LeaderSelector
        LeaderSelector leaderSelector = new LeaderSelector(this.curatorFramework, this.electionPath, new LeaderSelectorListenerAdapter() {
            private WorkingThread workingThread;
            private boolean shouldStop;

            public void takeLeadership(CuratorFramework client) throws Exception {
                //定义成功抢占主节点（leadership）之后的操作
                LOG.info("take leadership!");
                shouldStop = false;
                workingThread = new WorkingThread();
                workingThread.start();
                //防止退出
                while (!shouldStop) {
                    TimeUnit.SECONDS.sleep(10);
                }
            }

            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                try {
                    super.stateChanged(client, newState);
                } catch (CancelLeadershipException e) {
                    //响应失去主节点（leadership）的情况：停止线程执行
                    LOG.info("CancelLeadershipException,try to release leadership");
                    workingThread.stopGracefully();
                    shouldStop = true;
                    LOG.info("release leadership successfully");
                }
            }
        });
        //By default, when LeaderSelectorListener.takeLeadership(CuratorFramework) returns, this instance is not requeued. Calling this method puts the leader selector into a mode where it will always requeue itself.
        leaderSelector.autoRequeue();
        //开始竞争leadership
        leaderSelector.start();
        //防止程序退出
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        System.setProperty("log4j.configurationFile", ElectionApp.class.getResource("/log4j.properties").getPath());
        System.setProperty("log4j2.disable.jmx", Boolean.TRUE.toString());
        String zkConnectionString = "vm1:2181,vm1:2182,vm1:2183";
        String electionPath = "/test/election";
        ElectionApp electionApp = new ElectionApp(zkConnectionString, electionPath);
        //初始化，直到成功为止
        while (true) {
            try {
                electionApp.init();
                break;
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
        //启动工作
        electionApp.startJob();
    }
}
