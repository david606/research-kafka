/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.record;

/**
 * 二进制格式，由4字节大小，8字节偏移量和记录字节组成
 * See {@link MemoryRecords} for the in-memory representation.
 */
public interface Records extends Iterable<LogEntry> {

    int SIZE_LENGTH = 4;
    int OFFSET_LENGTH = 8;
    /**
     * 记录开销SIZE_LENGTH + OFFSET_LENGTH
     */
    int LOG_OVERHEAD = SIZE_LENGTH + OFFSET_LENGTH;

    /**
     * The size of these records in bytes
     */
    int sizeInBytes();

}
