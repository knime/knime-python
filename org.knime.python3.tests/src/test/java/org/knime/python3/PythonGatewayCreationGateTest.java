/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Jun 3, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.knime.conda.envbundling.action.InstallCondaEnvironment;

/**
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class PythonGatewayCreationGateTest {

    private static PythonGatewayCreationGate GATE = PythonGatewayCreationGate.INSTANCE;

    @Test
    public void testBlock() {
        assertTrue(GATE.isPythonGatewayCreationAllowed());
        GATE.blockPythonCreation();
        assertFalse(GATE.isPythonGatewayCreationAllowed());
        GATE.allowPythonCreation();
        assertTrue(GATE.isPythonGatewayCreationAllowed());
    }

    @Test
    public void testBlockUnblockBlock() {
        assertTrue(GATE.isPythonGatewayCreationAllowed());
        GATE.blockPythonCreation();
        assertFalse(GATE.isPythonGatewayCreationAllowed());
        GATE.allowPythonCreation();
        assertTrue(GATE.isPythonGatewayCreationAllowed());
        GATE.blockPythonCreation();
        assertFalse(GATE.isPythonGatewayCreationAllowed());
        GATE.allowPythonCreation();
        assertTrue(GATE.isPythonGatewayCreationAllowed());
    }

    @Test
    public void testBlockTwice() {
        assertTrue(GATE.isPythonGatewayCreationAllowed());
        GATE.blockPythonCreation();
        GATE.blockPythonCreation();
        assertFalse(GATE.isPythonGatewayCreationAllowed());
        GATE.allowPythonCreation();
        assertFalse(GATE.isPythonGatewayCreationAllowed());
        GATE.allowPythonCreation();
        assertTrue(GATE.isPythonGatewayCreationAllowed());
    }

    @Test
    public void testListener() {
        var counter = new AtomicInteger(0);
        var listener = new PythonGatewayCreationGate.PythonGatewayCreationGateListener() {
            @Override
            public void onPythonGatewayCreationGateOpen() {
                assertTrue(GATE.isPythonGatewayCreationAllowed());
                assertTrue(counter.get() % 2 == 0); // opened at 2 and 4
            }

            @Override
            public void onPythonGatewayCreationGateClose() {
                assertFalse(GATE.isPythonGatewayCreationAllowed());
                assertTrue(counter.get() % 2 == 1); // blocked at 1 and 3
                assertTrue(counter.get() > 0);
            }
        };

        GATE.registerListener(listener);
        counter.incrementAndGet(); // 1
        GATE.blockPythonCreation();
        counter.incrementAndGet(); // 2
        GATE.allowPythonCreation();
        counter.incrementAndGet(); // 3
        GATE.blockPythonCreation();
        counter.incrementAndGet(); // 4
        GATE.allowPythonCreation();
        GATE.deregisterListener(listener);
    }

    @Test(timeout = 100)
    public void testAwaitReturnsImmediately() throws InterruptedException {
        GATE.awaitPythonGatewayCreationAllowedInterruptibly();
    }

    @Test(timeout = 100)
    public void testAwaitBlocksUntilCreationAllowed() throws InterruptedException {
        var latch = new CountDownLatch(1);
        GATE.blockPythonCreation();

        final var exec = Executors.newSingleThreadExecutor();
        exec.submit(() -> {
            try {
                GATE.awaitPythonGatewayCreationAllowedInterruptibly();
            } catch (InterruptedException e) {
                fail("Test got interrupted");
            }
            assertEquals(0L, latch.getCount());
        });

        Thread.sleep(20); //NOSONAR
        GATE.allowPythonCreation();
        latch.countDown();
    }

    @Test
    public void testCondaInstallListenerBlocksAndUnblocksGate() throws Exception {
        // Ensure initial state
        assertTrue("Gate should be open initially", GATE.isPythonGatewayCreationAllowed());

        // Simulate a long-running install using latches
        CountDownLatch installStarted = new CountDownLatch(1);
        CountDownLatch installContinue = new CountDownLatch(1);
        AtomicInteger startCalled = new AtomicInteger(0);
        AtomicInteger endCalled = new AtomicInteger(0);

        // Register a test listener that blocks in onInstallStart
        var testListener = new InstallCondaEnvironment.EnvironmentInstallListener() {
            @Override
            public void onInstallStart(final String name) {
                startCalled.incrementAndGet();
                installStarted.countDown();
                try {
                    installContinue.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onInstallEnd(final String name) {
                endCalled.incrementAndGet();
            }
        };
        InstallCondaEnvironment.registerEnvironmentInstallListener(testListener);

        // Start install in a separate thread
        Thread installThread = new Thread(() -> {
            var installAction = new InstallCondaEnvironment.InstallAction();
            var map = new HashMap<String, Object>();
            map.put("directory", Paths.get("").toAbsolutePath().toString());
            map.put("name", "Testenv");
            // This will fail because there's no extension at this path,
            // but we don't care here as we only want the listener to be triggered
            installAction.execute(map);
        });
        installThread.start();

        // Wait for install to start
        assertTrue("Install did not start in time", installStarted.await(2, java.util.concurrent.TimeUnit.SECONDS));
        // Should be blocked now
        assertFalse("Gate should be blocked during install", GATE.isPythonGatewayCreationAllowed());
        assertEquals("onInstallStart should be called once", 1, startCalled.get());
        // Allow install to continue
        installContinue.countDown();
        installThread.join(2000);
        // Should be allowed again
        assertTrue("Gate should be open after install", GATE.isPythonGatewayCreationAllowed());
        assertEquals("onInstallEnd should be called once", 1, endCalled.get());
        InstallCondaEnvironment.deregisterEnvironmentInstallListener(testListener);
    }
}
