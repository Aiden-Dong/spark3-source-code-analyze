/*
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

package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

/**
 * 管理可调整大小的内存区域的簿记操作。此类是 [[MemoryManager]] 的内部类。
 * 有关更多详细信息，请参阅子类。
 *
 * @param lock 一个 [[MemoryManager]] 实例，用于同步。我们故意将类型擦除为 `Object`
 *      以避免编程错误，因为此对象应该仅用于同步目的。
 */
private[memory] abstract class MemoryPool(lock: Object) {

  @GuardedBy("lock")
  private[this] var _poolSize: Long = 0  // 当前内存池大小


  final def poolSize: Long = lock.synchronized {
    _poolSize
  }

  // 当前已经使用的内存字节大小
  def memoryUsed: Long

  // 返回单签生育的内存数量
  final def memoryFree: Long = lock.synchronized {
    _poolSize - memoryUsed
  }

  // 扩增当前内存资源池的字节大小
  final def incrementPoolSize(delta: Long): Unit = lock.synchronized {
    require(delta >= 0)
    _poolSize += delta
  }

  // 释放当前内存资源池的大小
  final def decrementPoolSize(delta: Long): Unit = lock.synchronized {
    require(delta >= 0)
    require(delta <= _poolSize)
    require(_poolSize - delta >= memoryUsed)
    _poolSize -= delta
  }
}
