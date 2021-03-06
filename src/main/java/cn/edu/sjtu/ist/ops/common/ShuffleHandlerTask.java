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

public class ShuffleHandlerTask {
    public static enum Type {
        PREPARE_SHUFFLE, SHUFFLECOMPLETED, COLLECTION
    }

    private Type type;
    private ShuffleCompletedConf shuffleC;
    private CollectionConf collection;
    private MapConf map;
    // private ShuffleConf shuffle;

    public ShuffleHandlerTask(MapConf map) {
        this.type = Type.PREPARE_SHUFFLE;
        this.map = map;
    }

    public ShuffleHandlerTask(ShuffleCompletedConf shuffleC) {
        this.type = Type.SHUFFLECOMPLETED;
        this.shuffleC = shuffleC;
    }

    public ShuffleHandlerTask(CollectionConf collection) {
        this.type = Type.COLLECTION;
        this.collection = collection;
    }

    public Type getType() {
        return this.type;
    }

    public CollectionConf getCollection() {
        return collection;
    }

    public ShuffleCompletedConf getShuffleC() {
        return shuffleC;
    }

    public MapConf getMap() {
        return map;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}