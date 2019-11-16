/*
 * Copyright 2018 SJTU IST Lab
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.sjtu.ist.ops;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.common.io.ByteSink;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import com.google.gson.Gson;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.sjtu.ist.ops.common.JobConf;
import cn.edu.sjtu.ist.ops.common.MapConf;
import cn.edu.sjtu.ist.ops.common.OpsConf;
import cn.edu.sjtu.ist.ops.common.OpsNode;
import cn.edu.sjtu.ist.ops.common.CollectionConf;
import cn.edu.sjtu.ist.ops.common.IndexReader;
import cn.edu.sjtu.ist.ops.common.IndexRecord;
import cn.edu.sjtu.ist.ops.common.ShuffleCompletedConf;
import cn.edu.sjtu.ist.ops.common.ShuffleHandlerTask;
import cn.edu.sjtu.ist.ops.common.ShuffleRichConf;
import cn.edu.sjtu.ist.ops.common.ShuffleConf;
import cn.edu.sjtu.ist.ops.common.MapTaskAlloc;
import cn.edu.sjtu.ist.ops.common.ReduceTaskAlloc;
import cn.edu.sjtu.ist.ops.util.EtcdService;
import cn.edu.sjtu.ist.ops.util.OpsUtils;
import cn.edu.sjtu.ist.ops.util.OpsWatcher;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class OpsShuffleHandler extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(OpsShuffleHandler.class);
    private final OpsNode host;
    private final Server workerServer;
    private final OpsConf opsConf;
    private volatile boolean stopped = false;
    private final Set<ShuffleRichConf> pendingShuffles = new HashSet<>();
    private final Set<ShuffleHandlerTask> pendingShuffleHandlerTasks = new HashSet<>();
    private final HashMap<String, JobConf> jobs = new HashMap<>();
    /** Maps from a job to the mapTaskAlloc */
    private final HashMap<String, MapTaskAlloc> mapTaskAllocMapping = new HashMap<String, MapTaskAlloc>();
    /** Maps from a job to the reduceTaskAlloc */
    private final HashMap<String, ReduceTaskAlloc> reduceTaskAllocMapping = new HashMap<String, ReduceTaskAlloc>();
    private final HashMap<String, List<MapConf>> completedMapsMapping = new HashMap<String, List<MapConf>>();
    private final HashMap<String, HashMap<String, IndexReader>> indexReaderMapping = new HashMap<>();
    private final Random random = new Random();
    private Gson gson = new Gson();

    // private final ManagedChannel masterChannel;
    // private final OpsShuffleDataGrpc.OpsShuffleDataStub masterStub;

    public OpsShuffleHandler(OpsConf opsConf, OpsNode host) {
        EtcdService.initClient();

        this.opsConf = opsConf;
        this.host = host;
        this.workerServer = ServerBuilder.forPort(this.opsConf.getPortWorkerGRPC()).addService(new OpsShuffleDataService())
                .build();
    }

    public void shutdown() throws InterruptedException {
        // masterChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        this.setName("ops-shuffle-handler");
        try {
            this.workerServer.start();
            logger.info("gRPC workerServer started, listening on " + this.opsConf.getPortWorkerGRPC());

            while (!stopped && !Thread.currentThread().isInterrupted()) {
                synchronized(this) {
                    wait();
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }

    private class OpsShuffleDataService extends OpsShuffleDataGrpc.OpsShuffleDataImplBase {
        @Override
        public StreamObserver<Page> transfer(StreamObserver<Ack> responseObserver) {
            return new StreamObserver<Page>() {
                String path = null;
                int count = 0;
                @Override
                public void onNext(Page page) {
                    try {
                        ByteString content = page.getContent();
                        File file = new File(page.getPath());
                        ByteSink byteSink = Files.asByteSink(file, FileWriteMode.APPEND);
                        byteSink.write(content.toByteArray());

                        if(path == null) {
                            path = page.getPath();
                        }
                        count++;
                        // logger.debug("Receive page: {Path: " + file.toString() + ", Length: " + file.length() + "}");
                    } catch (Exception e){
                        e.printStackTrace();

                        logger.error("transfer error.");
                        this.onError(e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.warn("Encountered error in exchange", t);
                    responseObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    logger.debug("Receive file: " + path + ", pages count: " + count);
                    Ack ack = Ack.newBuilder().setIsDone(true).build();
                    responseObserver.onNext(ack);
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
