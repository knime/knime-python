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
 *   Aug 9, 2024 (adrian.nembach): created
 */
package org.knime.python3.nodes.ports.converters;

import org.knime.core.node.port.PortObjectSpec;
import org.knime.python3.types.port.PortObjectSpecConversionContext;
import org.knime.python3.types.port.PythonPortObjectSpec;
import org.knime.python3.types.port.PythonToKnimePortObjectSpecConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public abstract class AbstractJsonPythonToKnimePortObjectSpecConverter<S extends PythonPortObjectSpec, T extends PortObjectSpec>
    implements PythonToKnimePortObjectSpecConverter<S, T> {

    @Override
    public T convert(final S source, final PortObjectSpecConversionContext context) {
        final var om = new ObjectMapper();
        try {
            final var rootNode = om.readTree(source.toJsonString());
            return parse(rootNode);
        } catch (JsonProcessingException ex) { // NOSONAR: if we don't split this block up, Eclipse doesn't like it for some reason
            throw new IllegalStateException(
                "Could not parse %s from given JSON data".formatted(getClass().getSimpleName()), ex);
        }
    }

    /**
     * @param rootNode of the JSON object received from the Python side
     * @return the parsed PortObjectSpec
     * @throws JsonProcessingException if the JSON can't be parsed
     */
    protected abstract T parse(final JsonNode rootNode) throws JsonProcessingException;

}
