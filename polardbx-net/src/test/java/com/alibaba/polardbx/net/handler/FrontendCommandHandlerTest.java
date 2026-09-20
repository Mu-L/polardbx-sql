/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.polardbx.net.handler;

import com.alibaba.polardbx.Commands;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FrontendCommandHandlerTest {

    @Mock
    private FrontendConnection mockConnection;

    @Mock
    private NIOProcessor mockProcessor;

    @Mock
    private CommandCount mockCommandCount;

    private FrontendCommandHandler target;

    @Before
    public void setUp() {
        when(mockConnection.getProcessor()).thenReturn(mockProcessor);
        when(mockProcessor.getCommands()).thenReturn(mockCommandCount);
        target = new FrontendCommandHandler(mockConnection);
    }

    @Test
    public void testPacketOutputStateIsResetBeforeCommand() {
        byte[] data = new byte[5];
        data[4] = Commands.COM_PING;

        target.handle(data);

        InOrder inOrder = Mockito.inOrder(mockConnection);
        inOrder.verify(mockConnection).setPacketId((byte) 0);
        inOrder.verify(mockConnection).resetPacketOutputState();
        inOrder.verify(mockConnection).ping();
        verify(mockCommandCount).doPing();
    }
}
