/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mpich.util;


import java.util.IllegalFormatException;
import java.util.Map;
import java.util.HashMap;

import org.junit.Assert;
import org.apache.hadoop.mpich.appmaster.pmi.ClientToServerCommand;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestUtils {


    @Rule
    public ExpectedException exception = ExpectedException.none();

    @org.junit.Test
    public void testGetCommand() {
        Map<String, String> testGet1 = new HashMap<String, String>();
        testGet1.put("cmd", "create_kvs");
        testGet1.put("mcmd", "init");
        Map<String, String> testGet2 = new HashMap<String, String>();
        testGet2.put("mcmd", "init");
        Map<String, String> testGet3 = new HashMap<String, String>();
        Assert.assertEquals(ClientToServerCommand.CREATE_KVS, Utils.getCommand(testGet1));
        Assert.assertEquals(ClientToServerCommand.INIT, Utils.getCommand(testGet2));
        Assert.assertEquals(ClientToServerCommand.UNRECOGNIZED, Utils.getCommand(testGet3));
    }

    @org.junit.Test
    public void testParseKeyVals() throws Exception {
        Map<String, String> testParse = new HashMap<String, String>();
        testParse.put("a", "1");
        testParse.put("d", "5");
        Assert.assertEquals(testParse, Utils.parseKeyVals("a=1 d=5"));
        exception.expect(Exception.class);
        exception.expectMessage("Parse message a=1=c d=5 failed");
        Utils.parseKeyVals("a=1=c d=5");
    }
}
