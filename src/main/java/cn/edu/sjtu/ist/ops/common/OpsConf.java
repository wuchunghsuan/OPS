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

import java.util.ArrayList;

public class OpsConf {
    private ArrayList<OpsNode> workers;
    private OpsNode master;
    private int portMasterGRPC = 14010;
    private int portWorkerGRPC = 14020;

    public OpsConf(OpsNode master, ArrayList<OpsNode> workers) {
        this.master = master;
        this.workers = workers;
    }

    public ArrayList<OpsNode> getWorkers() {
        return this.workers;
    }

    public void setWorkers(ArrayList<OpsNode> workers) {
        this.workers = workers;
    }

    public OpsNode getMaster() {
        return this.master;
    }

    public void setMaster(OpsNode master) {
        this.master = master;
    }

    public int getPortMasterGRPC() {
        return this.portMasterGRPC;
    }

    public int getPortWorkerGRPC() {
        return this.portWorkerGRPC;
    }
    
}