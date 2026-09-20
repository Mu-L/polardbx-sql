package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.corrector.Reporter;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.gsi.corrector.GsiChecker;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CheckGsiTaskTest {
    @Test
    public void testCheckInBackfill_1() {
        ExecutionContext ec = mock(ExecutionContext.class);
        CheckGsiTask checkGsiTask = mock(CheckGsiTask.class);
        when(checkGsiTask.isUseFastChecker(ec)).thenReturn(true);
        when(checkGsiTask.fastCheckWithCatchEx(ec)).thenReturn(true);
        doCallRealMethod().when(checkGsiTask).checkInBackfill(ec);
        checkGsiTask.checkInBackfill(ec);
    }

    @Test(expected = TddlNestableRuntimeException.class)
    public void testCheckInBackfill_2() {
        ExecutionContext ec = mock(ExecutionContext.class);
        CheckGsiTask checkGsiTask = mock(CheckGsiTask.class);
        when(checkGsiTask.isUseFastChecker(ec)).thenReturn(false);
        when(checkGsiTask.isOnlyUseFastChecker(ec)).thenReturn(true);
        doCallRealMethod().when(checkGsiTask).checkInBackfill(ec);
        checkGsiTask.checkInBackfill(ec);
    }

    @Test(expected = TddlNestableRuntimeException.class)
    public void testCheckInBackfill_3() {
        Map<String, String> srcCheckColumnMap = new HashMap<>();
        srcCheckColumnMap.put("a", "b");
        ExecutionContext ec = mock(ExecutionContext.class);
        CheckGsiTask checkGsiTask = mock(CheckGsiTask.class);
        when(checkGsiTask.isUseFastChecker(ec)).thenReturn(false);
        when(checkGsiTask.isOnlyUseFastChecker(ec)).thenReturn(false);
        when(checkGsiTask.getSrcCheckColumnMap()).thenReturn(srcCheckColumnMap);
        doCallRealMethod().when(checkGsiTask).checkInBackfill(ec);
        checkGsiTask.checkInBackfill(ec);
    }

    @Test(expected = TddlNestableRuntimeException.class)
    public void testCheckInBackfill_4() {
        Map<String, String> checkColumnMap = new HashMap<>();
        ExecutionContext ec = mock(ExecutionContext.class);
        CheckGsiTask checkGsiTask = mock(CheckGsiTask.class);
        Checker checker = mock(Checker.class);
        GsiChecker.Params checkerParams = mock(GsiChecker.Params.class);
        when(checkGsiTask.isUseFastChecker(ec)).thenReturn(false);
        when(checkGsiTask.isOnlyUseFastChecker(ec)).thenReturn(false);
        when(checkGsiTask.getSrcCheckColumnMap()).thenReturn(checkColumnMap);
        when(checkGsiTask.getDstCheckColumnMap()).thenReturn(checkColumnMap);
        when(checkGsiTask.getCheckParams()).thenReturn(checkerParams);
        when(checkGsiTask.getCheckParams().getEarlyFailNumber()).thenReturn(1L);
        when(checkGsiTask.buildChecker(ec)).thenReturn(checker);
        when(ec.copy()).thenReturn(ec);
        Mockito.doThrow(new TddlNestableRuntimeException("Too many conflicts"))
            .when(checker).check(any(), any());
        doCallRealMethod().when(checkGsiTask).checkInBackfill(ec);
        checkGsiTask.checkInBackfill(ec);
    }

    @Test(expected = TddlNestableRuntimeException.class)
    public void testCheckInBackfill_5() {
        Map<String, String> checkColumnMap = new HashMap<>();
        ExecutionContext ec = mock(ExecutionContext.class);
        CheckGsiTask checkGsiTask = mock(CheckGsiTask.class);
        Checker checker = mock(Checker.class);
        GsiChecker.Params checkerParams = mock(GsiChecker.Params.class);
        when(checkGsiTask.isUseFastChecker(ec)).thenReturn(false);
        when(checkGsiTask.isOnlyUseFastChecker(ec)).thenReturn(false);
        when(checkGsiTask.getSrcCheckColumnMap()).thenReturn(checkColumnMap);
        when(checkGsiTask.getDstCheckColumnMap()).thenReturn(checkColumnMap);
        when(checkGsiTask.getCheckParams()).thenReturn(checkerParams);
        when(checkGsiTask.getCheckParams().getEarlyFailNumber()).thenReturn(1L);
        when(checkGsiTask.buildChecker(ec)).thenReturn(checker);
        when(ec.copy()).thenReturn(ec);
        Mockito.doThrow(new TddlNestableRuntimeException("else"))
            .when(checker).check(any(), any());
        doCallRealMethod().when(checkGsiTask).checkInBackfill(ec);
        checkGsiTask.checkInBackfill(ec);
    }

    @Test(expected = TddlNestableRuntimeException.class)
    public void testCheckInBackfill_6() {
        List<CheckerManager.CheckerReport> checkerReports = new ArrayList<>();
        checkerReports.add(new CheckerManager.CheckerReport());
        try (MockedConstruction<Reporter> mocked = mockConstruction(Reporter.class, (mock, context) -> {
            when(mock.getCheckerReports()).thenReturn(checkerReports);
        })) {
            Map<String, String> checkColumnMap = new HashMap<>();
            ExecutionContext ec = mock(ExecutionContext.class);
            CheckGsiTask checkGsiTask = mock(CheckGsiTask.class);
            Checker checker = mock(Checker.class);
            GsiChecker.Params checkerParams = mock(GsiChecker.Params.class);
            when(checkGsiTask.isUseFastChecker(ec)).thenReturn(false);
            when(checkGsiTask.isOnlyUseFastChecker(ec)).thenReturn(false);
            when(checkGsiTask.getSrcCheckColumnMap()).thenReturn(checkColumnMap);
            when(checkGsiTask.getDstCheckColumnMap()).thenReturn(checkColumnMap);
            when(checkGsiTask.getCheckParams()).thenReturn(checkerParams);
            when(checkGsiTask.getCheckParams().getEarlyFailNumber()).thenReturn(1L);
            when(checkGsiTask.buildChecker(ec)).thenReturn(checker);
            when(ec.copy()).thenReturn(ec);
            doNothing().when(checker).check(any(), any());
            doCallRealMethod().when(checkGsiTask).checkInBackfill(ec);
            checkGsiTask.checkInBackfill(ec);
        }
    }

    @Test()
    public void testCheckInBackfill_7() {
        List<CheckerManager.CheckerReport> checkerReports = new ArrayList<>();
        try (MockedConstruction<Reporter> mocked = mockConstruction(Reporter.class, (mock, context) -> {
            when(mock.getCheckerReports()).thenReturn(checkerReports);
        })) {
            Map<String, String> checkColumnMap = new HashMap<>();
            ExecutionContext ec = mock(ExecutionContext.class);
            CheckGsiTask checkGsiTask = mock(CheckGsiTask.class);
            Checker checker = mock(Checker.class);
            GsiChecker.Params checkerParams = mock(GsiChecker.Params.class);
            when(checkGsiTask.isUseFastChecker(ec)).thenReturn(false);
            when(checkGsiTask.isOnlyUseFastChecker(ec)).thenReturn(false);
            when(checkGsiTask.getSrcCheckColumnMap()).thenReturn(checkColumnMap);
            when(checkGsiTask.getDstCheckColumnMap()).thenReturn(checkColumnMap);
            when(checkGsiTask.getCheckParams()).thenReturn(checkerParams);
            when(checkGsiTask.getCheckParams().getEarlyFailNumber()).thenReturn(1L);
            when(checkGsiTask.buildChecker(ec)).thenReturn(checker);
            when(ec.copy()).thenReturn(ec);
            doNothing().when(checker).check(any(), any());
            doCallRealMethod().when(checkGsiTask).checkInBackfill(ec);
            checkGsiTask.checkInBackfill(ec);
        }
    }

}
