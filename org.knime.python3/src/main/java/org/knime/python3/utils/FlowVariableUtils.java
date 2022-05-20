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
 *   20 May 2022 (Carsten Haubold): created
 */
package org.knime.python3.utils;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.node.workflow.VariableTypeRegistry;

/**
 * Utilities to filter and convert {@link FlowVariable}s between KNIME and Python
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class FlowVariableUtils {

    public static VariableType<?>[] convertToFlowVariableTypes(final Set<Class<?>> types) {
        return Arrays.stream(VariableTypeRegistry.getInstance().getAllTypes())
            .filter(v -> types.contains(v.getSimpleType())).toArray(VariableType[]::new);
    }

    public static LinkedHashMap<String, Object> convertToMap(final Collection<FlowVariable> flowVariables) {
        final LinkedHashMap<String, Object> flowVariablesMap = new LinkedHashMap<>(flowVariables.size());
        for (final FlowVariable variable : flowVariables) {
            // Flow variables typically contain Java primitives or strings as values (or arrays of these). We simply let
            // py4j handle the conversion of the values into their Python equivalents.
            // Note that the legacy Python back end only supports double, int, and string flow variables and converts
            // all other variable values, including arrays, into strings. So this simple implementation here is already
            // an improvement over the legacy implementation.
            //
            final VariableType<?> type = variable.getVariableType();
            Object value = variable.getValue(type);
            flowVariablesMap.put(variable.getName(), value);
        }
        return flowVariablesMap;
    }

    public static Collection<FlowVariable> convertFromMap(final Map<String, Object> flowVariablesMap,
        final NodeLogger logger) {
        final VariableType<?>[] allVariableTypes = VariableTypeRegistry.getInstance().getAllTypes();
        final Set<FlowVariable> flowVariables = new LinkedHashSet<>(flowVariablesMap.size());
        for (final var entry : flowVariablesMap.entrySet()) {
            final String variableName = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof List) {
                // py4j returns lists instead of arrays, convert them manually.
                value = convertIntoArrayValue(variableName, (List<?>)value, logger);
                if (value == null) {
                    continue;
                }
            }
            if (value != null) {
                final VariableType<?> matchingType = findMatchingVariableType(variableName, value, allVariableTypes, logger);
                if (matchingType != null
                    // Reserved flow variables like "knime.workspace" are also passed through the node, filter them out.
                    && isValidVariableName(variableName)) {
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    final var variable = new FlowVariable(variableName, (VariableType)matchingType, value);
                    flowVariables.add(variable);
                }
            } else {
                logger.warn("Flow variable '" + variableName + "' is empty. The variable will be ignored.");
            }
        }
        return flowVariables;
    }

    private static Object convertIntoArrayValue(final String variableName, final List<?> listValue, final NodeLogger logger) {
        if (!listValue.isEmpty()) {
            try {
                return listValue.toArray(size -> (Object[])Array.newInstance(listValue.get(0).getClass(), size));
            } catch (final ArrayStoreException ex) {
                logger.warn(
                    "Array-typed flow variable '" + variableName
                        + "' contains elements of different types, which is not allowed. The variable will be ignored",
                    ex);
            }
        } else {
            logger.warn("Array-typed flow variable '" + variableName + "' is empty and will be ignored.");
        }
        return null;
    }

    private static VariableType<?> findMatchingVariableType(final String variableName, final Object value,
        final VariableType<?>[] variableTypes, final NodeLogger logger) {
        VariableType<?> matchingType = null;
        for (final var type : variableTypes) {
            if (type.getSimpleType().isInstance(value)) {
                matchingType = type;
                break;
            }
        }
        if (matchingType == null) {
            logger.warn("KNIME offers no flow variable types that match the type of flow variable '" + variableName
                + "'. The variable will be ignored. Please change its type to something KNIME understands.");
            logger.debug(
                "The Java type of flow variable '" + variableName + "' is '" + value.getClass().getTypeName() + "'.");
        }
        return matchingType;
    }

    private static boolean isValidVariableName(final String variableName) {
        return !(variableName.startsWith(FlowVariable.Scope.Global.getPrefix())
            || variableName.startsWith(FlowVariable.Scope.Local.getPrefix()));
    }
}
