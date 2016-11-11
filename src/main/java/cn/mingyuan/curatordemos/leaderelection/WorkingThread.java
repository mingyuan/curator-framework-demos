package cn.mingyuan.curatordemos.leaderelection;

import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * 工作线程
 * @author jiangmingyuan@myhaowai.com
 * @version 2016/11/11 16:55
 * @since jdk1.8
 */
public class WorkingThread extends Thread {
    private static final Logger LOG = Logger.getLogger(Logger.class);
    private boolean shouldStop;

    @Override
    public void run() {
        while (!shouldStop) {
            LOG.info("I'M WORKING");
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                LOG.info("working thread interrupted,stop working.");
                break;
            }
        }
    }

    /**
     * 以优雅的方式停止线程执行
     */
    public void stopGracefully() {
        LOG.info("begin stop current thread");
        shouldStop = true;
        this.interrupt();
        try {
            this.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("stop current thread successfully");
    }
}
