/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.benchmark.driver.tdengine;


public class Config {
    public String database;
    public int maxBatchSize;
    public int varcharLen;
    public String jdbcURL;
    public int stt_trigger;
    public int buffer;
    public int replica;
    public boolean useStmt;
    public int walLevel;
    public int walFsyncPeriod;
    public int walRetentionPeriod;
    public int walRetentionSize;
    public int walRollPeriod;
    public int walSegSize;
    public String strict;
}

//        wal_level: 1
//        wal_fsync_period: 3000
//        wal_retention_period: 0
//        wal_retention_size: 0
//        wal_roll_period: 0
//        wal_seg_size: 0
//        strict: off