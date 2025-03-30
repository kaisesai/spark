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

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.ExternalAppendOnlyMap

/**
 * :: DeveloperApi ::
 * A set of functions used to aggregate data.
 *
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */
@DeveloperApi
case class Aggregator[K, V, C] (
    createCombiner: V => C, // 创建联合器
    mergeValue: (C, V) => C, // 合并数据
    mergeCombiners: (C, C) => C) { // 合并多个合并器结果

  def combineValuesByKey(
      iter: Iterator[_ <: Product2[K, V]],
      context: TaskContext): Iterator[(K, C)] = {
    // 联合器
    val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners);
    // 插入迭代器
    combiners.insertAll(iter);
    updateMetrics(context, combiners);
    // 返回迭代器
    combiners.iterator;
  }

  def combineCombinersByKey(
      iter: Iterator[_ <: Product2[K, C]],
      context: TaskContext): Iterator[(K, C)] = {
    // 合并器
    val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners);
    // 合并器添加迭代器
    combiners.insertAll(iter);
    updateMetrics(context, combiners);
    // 最后返回合并器的迭代器
    combiners.iterator;
  }

  /** Update task metrics after populating the external map. */
  private def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    }
  }
}
