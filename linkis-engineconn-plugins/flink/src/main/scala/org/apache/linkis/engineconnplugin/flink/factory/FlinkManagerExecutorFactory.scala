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

package org.apache.linkis.engineconnplugin.flink.factory

import org.apache.linkis.engineconn.common.creation.EngineCreationContext
import org.apache.linkis.engineconn.common.engineconn.EngineConn
import org.apache.linkis.engineconn.once.executor.OnceExecutor
import org.apache.linkis.engineconn.once.executor.creation.OnceExecutorFactory
import org.apache.linkis.engineconnplugin.flink.config.FlinkEnvConfiguration
import org.apache.linkis.engineconnplugin.flink.context.FlinkEngineConnContext
import org.apache.linkis.engineconnplugin.flink.executor.FlinkManagerConcurrentExecutor
import org.apache.linkis.engineconnplugin.flink.factory.FlinkManagerExecutorFactory.setDefaultExecutor
import org.apache.linkis.manager.label.entity.Label
import org.apache.linkis.manager.label.entity.engine.RunType._

class FlinkManagerExecutorFactory extends OnceExecutorFactory {

  override protected def newExecutor(
      id: Int,
      engineCreationContext: EngineCreationContext,
      engineConn: EngineConn,
      labels: Array[Label[_]]
  ): OnceExecutor = engineConn.getEngineConnSession match {
    case flinkEngineConnContext: FlinkEngineConnContext =>
      val executor = new FlinkManagerConcurrentExecutor(
        id,
        FlinkEnvConfiguration.FLINK_MANAGER_LOAD_TASK_MAX.getValue,
        flinkEngineConnContext
      )
      setDefaultExecutor(executor)
      executor
  }

  // just set lots of runType, but now only sql is supported.
  override protected def getSupportRunTypes: Array[String] =
    Array(JSON.toString)

  override protected def getRunType: RunType = JSON
}

object FlinkManagerExecutorFactory {

  private var defaultExecutor: FlinkManagerConcurrentExecutor = _

  def setDefaultExecutor(executor: FlinkManagerConcurrentExecutor): Unit = {
    defaultExecutor = executor
  }

  def getDefaultExecutor(): FlinkManagerConcurrentExecutor = defaultExecutor

}
