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
 *   Oct 14, 2024 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.ports.extension;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public final class ClassHierarchyMapTest {

    private interface TestTypeBound {
    }

    private interface TestInterfaceA extends TestTypeBound {

    }

    private interface TestInterfaceB extends TestTypeBound {

    }

    private interface TestSuperInterface extends TestTypeBound {

    }

    private interface UnrelatedInterface {

    }

    private class TestSuperClass implements TestSuperInterface {

    }

    private class TestClass extends TestSuperClass implements TestInterfaceA, TestInterfaceB, UnrelatedInterface {
    }

    private ClassHierarchyMap<TestTypeBound, String> m_classHierarchyMap;

    /**
     * Initializes a ClassHierarchyMap for the subsequent tests.
     */
    @Before
    public void setUp() {
        m_classHierarchyMap = new ClassHierarchyMap<>(TestTypeBound.class);
    }

    /**
     * Tests if {@link ClassHierarchyMap#put(Class, Object)} behaves as expected.
     */
    @Test
    public void testPut() {
        assertNull("Put should return null if no value is registered for a class, yet.",
            m_classHierarchyMap.put(TestClass.class, "foo"));
        assertEquals("Get should return the value registered for a class.", "foo",
            m_classHierarchyMap.getHierarchyAware(TestClass.class));
        assertEquals("Put with a class that is already in the map should return the prior value.", "foo",
            m_classHierarchyMap.put(TestClass.class, "bar"));
        assertEquals("Get should return the value registered for a given class.", "bar",
            m_classHierarchyMap.getHierarchyAware(TestClass.class));
        assertNull("Adding a value for a super class should not overwrite the value for a subclass.",
            m_classHierarchyMap.put(TestSuperClass.class, "baz"));
    }

    /**
     * Tests that {@link ClassHierarchyMap#getHierarchyAware(Class)} follows the correct class hierarchy traversal order
     * i.e. Class -> Interfaces the class implements (in order of declaration) -> super class -> interfaces of super
     * class and so on.
     */
    @Test
    public void testTraversalOrder() {
        m_classHierarchyMap.put(TestSuperInterface.class, "super interface");
        assertEquals("super interface", m_classHierarchyMap.getHierarchyAware(TestClass.class));
        m_classHierarchyMap.put(TestSuperClass.class, "super class");
        assertEquals("super class", m_classHierarchyMap.getHierarchyAware(TestClass.class));
        m_classHierarchyMap.put(TestInterfaceB.class, "interface B");
        assertEquals("interface B", m_classHierarchyMap.getHierarchyAware(TestClass.class));
        m_classHierarchyMap.put(TestTypeBound.class, "type bound");
        assertEquals("Interface A extends TestTypeBound, so TypeBound should be found before InterfaceB", "type bound",
            m_classHierarchyMap.getHierarchyAware(TestClass.class));
        m_classHierarchyMap.put(TestInterfaceA.class, "interface A");
        assertEquals("interface A", m_classHierarchyMap.getHierarchyAware(TestClass.class));
        m_classHierarchyMap.put(TestClass.class, "class");
        assertEquals("class", m_classHierarchyMap.getHierarchyAware(TestClass.class));
    }

}
