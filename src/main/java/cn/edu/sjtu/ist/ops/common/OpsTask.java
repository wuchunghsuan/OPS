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

package cn.edu.sjtu.ist.ops.common;

import com.google.gson.Gson;

public class OpsTask {
    public static enum Type {
        SCHEDULE, ONSHUFFLE, REGISTER_REDUCE
    }

    private Type type;
    private JobConf pendingJob;
    private MapConf pendingMap;
    private ReduceConf pendingReduce;
    private Integer reduceNum;

    public OpsTask(JobConf job) {
        this.type = Type.SCHEDULE;
        this.pendingJob = job;
    }

    public OpsTask(MapConf pendingTask) {
        this.type = Type.ONSHUFFLE;
        this.pendingMap = pendingTask;
    }

    public OpsTask(ReduceConf reduce, Integer reduceNum) {
        this.type = Type.REGISTER_REDUCE;
        this.pendingReduce = reduce;
        this.reduceNum = reduceNum;
    }

    public Type getType() {
        return this.type;
    }

    public JobConf getPendingJob() {
        return this.pendingJob;
    }

    public MapConf getPendingMap() {
        return this.pendingMap;
    }

    public ReduceConf getPendingReduce() {
        return this.pendingReduce;
    }

    public Integer getReduceNum() {
        return this.reduceNum;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}