/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datis.zookafka.client;

import java.io.Closeable;
import java.io.IOException;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jeus
 */
public class Test1 implements Watcher, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Test1.class);
    private volatile boolean expired = false;
    private volatile boolean connected = false;
    private String hostInfo = "172.17.0.8:2181";
    ZooKeeper zk;

    public Test1() {
    }

    public Test1(String hostInfo) {
        this.hostInfo = hostInfo;
    }

    void startZK() throws IOException {
        System.out.println("HOST INFORMATION:" + hostInfo);
        zk = new ZooKeeper(hostInfo, 15000, this);
        System.out.println(zk.getState());
        System.out.println(zk.getState().name());
    }

    boolean isConnected() {
        return connected;
    }

    public void testCreate(Object path) {
        String newZnode = path == null ? "/test1" : (String) path;
        createParent(newZnode, new byte[0]);
//        createParent("/assign", new byte[0]);
//        createParent("/tasks", new byte[0]);
//        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createParentCallback,
                data);
        System.out.println("CALLBACK IS:" + createParentCallback.toString());
    }
    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            System.out.println("Callback name:" + name);
            System.out.println("Callback object :" + ctx.toString());
            System.out.println("Callback path:" + path);
            System.out.println("Callback rc:" + rc);
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a 
                 * NODEEXISTS event back.
                     */
                    createParent(path, (byte[]) ctx);

                    break;
                case OK:
                    System.out.println("PARENT CREATED");
                    LOG.info("Parent created");

                    break;
                case NODEEXISTS:
                    LOG.warn("Parent already registered: " + path);

                    break;
                default:
                    LOG.error("Something went wrong: ",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    public static void main(String args[]) throws Exception {
        Test1 test1;
        if (args.length == 1) {
            System.out.println("Has Input argument:" + args[0]);
            test1 = new Test1(args[0]);
            test1.startZK();
        } else {
            System.out.println("not input argument");
            test1 = new Test1();
        }

        test1.startZK();

        while (!test1.isConnected()) {
            Thread.sleep(100);
        }
        test1.testCreate("/JeusTest");

        while (!test1.isExpired()) {
            Thread.sleep(1000);
        }
    }

    boolean isExpired() {
//        System.out.println("Expire is:"+expired);
//        System.out.println("State IsAlive:"+zk.getState().isAlive());
//        System.out.println("State Is Connected:"+zk.getState().isConnected());
//        System.out.println("State name:"+zk.getState().name());
//        System.out.println("State toString:"+zk.getState().toString());
        return expired;
    }

    @Override
    public void process(WatchedEvent e) {
        LOG.info("Processing event: " + e.toString());
        if (e.getType() == Event.EventType.None) {
            switch (e.getState()) {
                case SyncConnected:
                    System.out.println("Event SynConnected");
                    connected = true;
                    break;
                case Disconnected:
                    System.out.println("Event Disconnected");
                    connected = false;
                    break;
                case Expired:
                    System.out.println("Event Expired");
                    expired = true;
                    connected = false;
                    LOG.error("Session expiration");
                default:
                    break;
            }
        }
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
