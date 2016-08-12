/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python.nodes.objectreader;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeCreationContext;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python.kernel.PythonKernel;
import org.knime.python.nodes.PythonNodeModel;
import org.knime.python.port.PickledObject;
import org.knime.python.port.PickledObjectPortObject;

/**
 * This is the model implementation.
 * 
 * 
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
class PythonObjectReaderNodeModel extends PythonNodeModel<PythonObjectReaderNodeConfig> {

	/**
	 * Constructor for the node model.
	 */
	protected PythonObjectReaderNodeModel() {
		super(new PortType[0], new PortType[] { PickledObjectPortObject.TYPE });
	}

	protected PythonObjectReaderNodeModel(final NodeCreationContext context) {
		this();
		URI uri;
		try {
			uri = context.getUrl().toURI();
		} catch (URISyntaxException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		if ((!uri.getScheme().equals("knime")) || (!uri.getHost().equals("LOCAL"))) {
			throw new RuntimeException("Only pickle files in the local workspace are supported.");
		}
		getConfig().setSourceCode(PythonObjectReaderNodeConfig.getDefaultSourceCode(uri.getPath()));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObject[] execute(PortObject[] inData, ExecutionContext exec) throws Exception {
		PythonKernel kernel = new PythonKernel();
		PickledObject object = null;
		try {
			kernel.putFlowVariables(PythonObjectReaderNodeConfig.getVariableNames().getFlowVariables(),
					getAvailableFlowVariables().values());
			String[] output = kernel.execute(getConfig().getSourceCode(), exec);
			setExternalOutput(new LinkedList<String>(Arrays.asList(output[0].split("\n"))));
			setExternalErrorOutput(new LinkedList<String>(Arrays.asList(output[1].split("\n"))));
			exec.createSubProgress(0.9).setProgress(1);
			Collection<FlowVariable> variables = kernel.getFlowVariables(PythonObjectReaderNodeConfig.getVariableNames().getFlowVariables());
			object = kernel.getObject(PythonObjectReaderNodeConfig.getVariableNames().getOutputObjects()[0], exec);
			exec.createSubProgress(0.1).setProgress(1);
	        addNewVariables(variables);
		} finally {
			kernel.close();
		}
		return new PortObject[] { new PickledObjectPortObject(object) };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		return new PortObjectSpec[] { null };
	}

	@Override
	protected PythonObjectReaderNodeConfig createConfig() {
		return new PythonObjectReaderNodeConfig();
	}

}
