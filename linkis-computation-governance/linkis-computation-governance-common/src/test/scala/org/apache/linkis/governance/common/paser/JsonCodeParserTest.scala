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

package org.apache.linkis.governance.common.paser

import org.junit.jupiter.api.{Assertions, DisplayName, Test}

class JsonCodeParserTest {

  @Test
  @DisplayName("parseTest")
  def parseTest(): Unit = {

    val jsonStr = "{\"nane\":\"hadoop\",\"jobId\":\"0001\"}"
    val parser = new JsonCodeParser
    val array = parser.parse(jsonStr)

    Assertions.assertTrue(array.size == 1)
  }

  @Test
  @DisplayName("noJsonParseTest")
  def noJsonParseTest(): Unit = {

    val jsonStr = "{\"nane\":\"hadoop\",\"jobId\":\"0001\""
    val parser = new JsonCodeParser
    val array = parser.parse(jsonStr)

    Assertions.assertTrue(array.length == 1)
  }

}
