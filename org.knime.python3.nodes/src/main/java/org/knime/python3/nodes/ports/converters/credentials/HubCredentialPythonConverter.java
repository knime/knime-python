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

import java.net.URI;

import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.python3.nodes.ports.converters.JsonConverterUtils;
import org.knime.python3.types.port.converter.PortObjectConversionContext;
import org.knime.python3.types.port.converter.PortObjectEncoder;
import org.knime.python3.types.port.converter.PortObjectSpecConversionContext;
import org.knime.python3.types.port.ir.JavaEmptyIntermediateRepresentation;
import org.knime.python3.types.port.ir.JavaStringIntermediateRepresentation;
import org.knime.python3.types.port.ir.PortObjectIntermediateRepresentation;
import org.knime.python3.types.port.ir.PortObjectSpecIntermediateRepresentation;
import org.knime.workflowservices.connection.AbstractHubAuthenticationPortObject;
import org.knime.workflowservices.connection.AbstractHubAuthenticationPortObjectSpec;

/**
 * Converts {@link AbstractHubAuthenticationPortObject Hub credentials} to and from Python.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
// TODO also implement py to knime
@SuppressWarnings("restriction")
public final class HubCredentialPythonConverter implements
    PortObjectEncoder<AbstractHubAuthenticationPortObject, AbstractHubAuthenticationPortObjectSpec> {

    @Override
    public PortObjectSpecIntermediateRepresentation encodePortObjectSpec(
        final AbstractHubAuthenticationPortObjectSpec spec, final PortObjectSpecConversionContext context) {
        var xmlContent = CredentialPythonConverter.getXmlContent((CredentialPortObjectSpec)spec);
        var json = JsonConverterUtils.createObjectNode();
        json.put("data", xmlContent);
        json.put("hub_url", spec.getHubURL().map(URI::toString).orElse(null));
        return new JavaStringIntermediateRepresentation(json.toString());
    }

    @Override
    public PortObjectIntermediateRepresentation encodePortObject(
        final AbstractHubAuthenticationPortObject portObject, final PortObjectConversionContext context) {
        return JavaEmptyIntermediateRepresentation.INSTANCE;
    }

    @Override
    public Class<AbstractHubAuthenticationPortObject> getPortObjectClass() {
        return AbstractHubAuthenticationPortObject.class;
    }

    @Override
    public Class<AbstractHubAuthenticationPortObjectSpec> getPortObjectSpecClass() {
        return AbstractHubAuthenticationPortObjectSpec.class;
    }

}
