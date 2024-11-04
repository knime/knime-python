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
 *   Sep 25, 2024 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.ports.converters.credentials;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.knime.core.node.ModelContent;
import org.knime.core.node.port.AbstractSimplePortObjectSpec.AbstractSimplePortObjectSpecSerializer;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.python3.nodes.ports.PythonPortObjects.PythonCredentialPortObjectSpec;
import org.knime.python3.nodes.ports.converters.JsonConverterUtils;
import org.knime.python3.types.port.converter.PortObjectEncoder;
import org.knime.python3.types.port.converter.PortObjectConversionContext;
import org.knime.python3.types.port.converter.PortObjectSpecConversionContext;
import org.knime.python3.types.port.converter.PortObjectDecoder;
import org.knime.python3.types.port.ir.EmptyIntermediateRepresentation;
import org.knime.python3.types.port.ir.PortObjectIntermediateRepresentation;
import org.knime.python3.types.port.ir.PortObjectSpecIntermediateRepresentation;
import org.knime.python3.types.port.ir.StringIntermediateRepresentation;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
public final class CredentialPythonConverter
    implements PortObjectEncoder<CredentialPortObject, CredentialPortObjectSpec>,
    PortObjectDecoder<CredentialPortObject, EmptyIntermediateRepresentation, CredentialPortObjectSpec, StringIntermediateRepresentation> {

    @Override
    public CredentialPortObjectSpec decodePortObjectSpec(final StringIntermediateRepresentation source,
        final PortObjectSpecConversionContext context) {
        var rootNode = JsonConverterUtils.parseJson(source);
        try {
            final String serializedXMLString = rootNode.get("data").asText();

            CredentialPortObjectSpec credentialPortObjectSpec =
                PythonCredentialPortObjectSpec.loadFromXMLCredentialPortObjectSpecString(serializedXMLString);
            var spec = new PythonCredentialPortObjectSpec(credentialPortObjectSpec);
            return spec.getPortObjectSpec();

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException ex) {
            throw new IllegalStateException("Could not parse PythonCredentialPortObject from given JSON data", ex);
        }
    }

    @Override
    public CredentialPortObject decodePortObject(final EmptyIntermediateRepresentation source,
        final CredentialPortObjectSpec spec, final PortObjectConversionContext context) {
        return new CredentialPortObject(spec);
    }

    @Override
    public PortObjectSpecIntermediateRepresentation encodePortObjectSpec(final CredentialPortObjectSpec spec,
        final PortObjectSpecConversionContext context) {
        var json = new ObjectMapper().createObjectNode();
        json.put("data", getXmlContent(spec));
        return new StringIntermediateRepresentation(json.toString());
    }

    static String getXmlContent(final CredentialPortObjectSpec spec) {
        ModelContent fakeConfig = new ModelContent("fakeConfig");

        AbstractSimplePortObjectSpecSerializer.savePortObjectSpecToModelSettings(spec, fakeConfig);
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            fakeConfig.saveToXML(byteArrayOutputStream);
            byteArrayOutputStream.close(); // NOSONAR we have to close here
            return byteArrayOutputStream.toString(StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new IllegalStateException("Could not save the PythonCredentialPortObjectSpec to XML.", ex);
        }
    }

    @Override
    public PortObjectIntermediateRepresentation encodePortObject(final CredentialPortObject portObject,
        final PortObjectConversionContext context) {
        return EmptyIntermediateRepresentation.INSTANCE;
    }

    @Override
    public Class<CredentialPortObject> getPortObjectClass() {
        return CredentialPortObject.class;
    }

    @Override
    public Class<CredentialPortObjectSpec> getPortObjectSpecClass() {
        return CredentialPortObjectSpec.class;
    }

}
