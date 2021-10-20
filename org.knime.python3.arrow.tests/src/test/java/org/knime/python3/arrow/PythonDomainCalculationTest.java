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
 *   Sep 27, 2021 (benjamin): created
 */
package org.knime.python3.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowColumnStoreFactory;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.data.columnar.domain.DefaultDomainWritableConfig;
import org.knime.core.data.columnar.domain.DomainWritableConfig;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.python3.PythonDataSink;
import org.knime.python3.PythonGateway;
import org.knime.python3.arrow.TestUtils.ArrowTestsEntryPoint;

/**
 * Tests for using the {@link DomainCalculator} with a {@link PythonDataSink}.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class PythonDomainCalculationTest {

    private ArrowColumnStoreFactory m_storeFactory;

    private BufferAllocator m_allocator;

    /** Create allocator and storeFactory */
    @Before
    public void before() {
        m_allocator = new RootAllocator();
        m_storeFactory = new ArrowColumnStoreFactory(m_allocator, 0, m_allocator.getLimit(),
            ArrowCompressionUtil.ARROW_NO_COMPRESSION);
    }

    /** Close allocator */
    @After
    public void after() {
        m_allocator.close();
    }

    /**
     * Test with double values.
     *
     * @throws Exception
     */
    @Test
    public void testDoubleDomain() throws Exception {
        final var readPath = TestUtils.createTmpKNIMEArrowPath();
        try (final PythonGateway<ArrowTestsEntryPoint> pythonGateway = TestUtils.openPythonGateway()) {
            final ArrowTestsEntryPoint entryPoint = pythonGateway.getEntryPoint();

            final var sink = PythonArrowDataUtils.createSink(readPath);

            final Supplier<DomainWritableConfig> configSupplier = () -> {
                final var schema = PythonArrowDataUtils.createColumnarValueSchema(sink);
                return new DefaultDomainWritableConfig(schema, 10, false);
            };

            try (final var domainCalculator =
                PythonArrowDataUtils.createDomainCalculator(sink, m_storeFactory, configSupplier)) {
                entryPoint.testDomainCalculation("double", sink);
                final var domain = domainCalculator.getDomain(1);
                assertTrue(domain.hasBounds());
                assertEquals(0.0, ((DoubleCell)domain.getLowerBound()).getDoubleValue(), 0.00001);
                assertEquals(1.0, ((DoubleCell)domain.getUpperBound()).getDoubleValue(), 0.00001);
            }
        }
    }

    /**
     * Test with integer values.
     *
     * @throws Exception
     */
    @Test
    public void testIntDomain() throws Exception {
        final var readPath = TestUtils.createTmpKNIMEArrowPath();
        try (final PythonGateway<ArrowTestsEntryPoint> pythonGateway = TestUtils.openPythonGateway()) {
            final ArrowTestsEntryPoint entryPoint = pythonGateway.getEntryPoint();

            final var sink = PythonArrowDataUtils.createSink(readPath);

            final Supplier<DomainWritableConfig> configSupplier = () -> {
                final var schema = PythonArrowDataUtils.createColumnarValueSchema(sink);
                return new DefaultDomainWritableConfig(schema, 10, false);
            };

            try (final var domainCalculator =
                PythonArrowDataUtils.createDomainCalculator(sink, m_storeFactory, configSupplier)) {
                entryPoint.testDomainCalculation("int", sink);
                final var domain = domainCalculator.getDomain(1);
                assertTrue(domain.hasBounds());
                assertEquals(0, ((IntCell)domain.getLowerBound()).getIntValue());
                assertEquals(499, ((IntCell)domain.getUpperBound()).getIntValue());
            }
        }
    }

    /**
     * Test with string values, should not have values because too many different strings are present.
     *
     * @throws Exception
     */
    @Test
    public void testStringDomain() throws Exception {
        final var readPath = TestUtils.createTmpKNIMEArrowPath();
        try (final PythonGateway<ArrowTestsEntryPoint> pythonGateway = TestUtils.openPythonGateway()) {
            final ArrowTestsEntryPoint entryPoint = pythonGateway.getEntryPoint();

            final var sink = PythonArrowDataUtils.createSink(readPath);

            final Supplier<DomainWritableConfig> configSupplier = () -> {
                final var schema = PythonArrowDataUtils.createColumnarValueSchema(sink);
                return new DefaultDomainWritableConfig(schema, 10, false);
            };

            try (final var domainCalculator =
                PythonArrowDataUtils.createDomainCalculator(sink, m_storeFactory, configSupplier)) {
                entryPoint.testDomainCalculation("string", sink);
                final var domain = domainCalculator.getDomain(1);
                assertFalse(domain.hasBounds());
                assertFalse(domain.hasValues());
            }
        }
    }

    /**
     * Test with categorical string values.
     *
     * @throws Exception
     */
    @Test
    public void testCategoricalStringDomain() throws Exception {
        final var readPath = TestUtils.createTmpKNIMEArrowPath();
        try (final PythonGateway<ArrowTestsEntryPoint> pythonGateway = TestUtils.openPythonGateway()) {
            final ArrowTestsEntryPoint entryPoint = pythonGateway.getEntryPoint();

            final var sink = PythonArrowDataUtils.createSink(readPath);

            final Supplier<DomainWritableConfig> configSupplier = () -> {
                final var schema = PythonArrowDataUtils.createColumnarValueSchema(sink);
                return new DefaultDomainWritableConfig(schema, 10, false);
            };

            try (final var domainCalculator =
                PythonArrowDataUtils.createDomainCalculator(sink, m_storeFactory, configSupplier)) {
                entryPoint.testDomainCalculation("categorical", sink);
                final var domain = domainCalculator.getDomain(1);
                assertFalse(domain.hasBounds());
                assertTrue(domain.hasValues());
                final var values = domain.getValues().stream().map(dataCell -> ((StringCell)dataCell).getStringValue())
                    .collect(Collectors.toSet());
                var expected = new HashSet<String>();
                expected.add("str0");
                expected.add("str1");
                expected.add("str2");
                expected.add("str3");
                expected.add("str4");
                assertEquals(expected, values);
            }
        }
    }
}
