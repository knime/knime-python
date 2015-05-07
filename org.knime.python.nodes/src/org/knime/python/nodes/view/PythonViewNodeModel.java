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
package org.knime.python.nodes.view;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import javax.imageio.ImageIO;

import org.knime.base.data.xml.SvgCell;
import org.knime.base.data.xml.SvgImageContent;
import org.knime.code.generic.ImageContainer;
import org.knime.core.data.image.png.PNGImageContent;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.port.image.ImagePortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.python.kernel.PythonKernel;
import org.knime.python.nodes.PythonNodeModel;

/**
 * This is the model implementation.
 * 
 * 
 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
 */
class PythonViewNodeModel extends PythonNodeModel<PythonViewNodeConfig> {
	
	private BufferedImage m_image;

	/**
	 * Constructor for the node model.
	 */
	protected PythonViewNodeModel() {
		super(new PortType[] { BufferedDataTable.TYPE }, new PortType[] { ImagePortObject.TYPE });
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObject[] execute(PortObject[] inData, ExecutionContext exec) throws Exception {
		PythonKernel kernel = new PythonKernel();
		ImageContainer image = null;
		try {
			kernel.putFlowVariables(PythonViewNodeConfig.getVariableNames().getFlowVariables(),
					getAvailableFlowVariables().values());
			kernel.putDataTable(PythonViewNodeConfig.getVariableNames().getInputTables()[0],
					(BufferedDataTable) inData[0], exec.createSubProgress(0.3));
			String[] output = kernel.execute(getConfig().getSourceCode(), exec);
			setExternalOutput(new LinkedList<String>(Arrays.asList(output[0].split("\n"))));
			setExternalErrorOutput(new LinkedList<String>(Arrays.asList(output[1].split("\n"))));
			exec.createSubProgress(0.4).setProgress(1);
			image = kernel.getImage(PythonViewNodeConfig.getVariableNames().getOutputImages()[0]);
			Collection<FlowVariable> variables = kernel.getFlowVariables(PythonViewNodeConfig.getVariableNames().getFlowVariables());
			exec.createSubProgress(0.3).setProgress(1);
	        addNewVariables(variables);
			m_image = image.getBufferedImage();
		} finally {
			kernel.close();
		}
		if (image.hasSvgDocument()) {
			return new PortObject[]{new ImagePortObject(new SvgImageContent(image.getSvgDocument()), new ImagePortObjectSpec(SvgCell.TYPE))};
		} else {
			return new PortObject[] { new ImagePortObject(new PNGImageContent(imageToBytes(image.getBufferedImage())),
					new ImagePortObjectSpec(PNGImageContent.TYPE)) };
		}
	}
	
	@Override
	protected void reset() {
		m_image = null;
		super.reset();
	}
	
	Image getOutputImage() {
		return m_image;
	}

	/**
	 * Returns the bytes of the given image.
	 * 
	 * @param image The image
	 * @return The image as byte array
	 * @throws IOException If an I/O error occurs
	 */
	private static byte[] imageToBytes(final BufferedImage image) throws IOException {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		ImageIO.write(image, "png", os);
		byte[] bytes = os.toByteArray();
		os.close();
		return bytes;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		return new PortObjectSpec[] { new ImagePortObjectSpec(PNGImageContent.TYPE) };
	}

    /**
     * The saved image is loaded.
     *
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        super.loadInternals(nodeInternDir, exec);
        File file = new File(nodeInternDir, "image.png");
        if (file.exists() && file.canRead()) {
        	m_image = ImageIO.read(file);
        }
    }

    /**
     * The created image is saved.
     *
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        super.saveInternals(nodeInternDir, exec);
        if (m_image != null) {
            File file = new File(nodeInternDir, "image.png");
            ImageIO.write(m_image, "png", file);
        }
    }
    
    @Override
    protected PythonViewNodeConfig createConfig() {
    	return new PythonViewNodeConfig();
    }

}
