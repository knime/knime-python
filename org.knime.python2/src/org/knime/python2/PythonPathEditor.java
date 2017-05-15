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
 */

package org.knime.python2;

import org.eclipse.jface.preference.FileFieldEditor;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;

public class PythonPathEditor extends Composite {
	
	private FileFieldEditor m_pathEditor;
	private Label m_info;
	private Label m_error;

	public PythonPathEditor(final String label, final Composite parent) {
		super(parent, SWT.NONE);
		GridData gridData = new GridData();
		gridData.grabExcessHorizontalSpace = true;
		gridData.grabExcessVerticalSpace = true;
		gridData.horizontalAlignment = SWT.FILL;
		gridData.verticalAlignment = SWT.FILL;
		setLayoutData(gridData);
		m_pathEditor = new FileFieldEditor(Activator.PLUGIN_ID, label, this);
		m_pathEditor.setStringValue(getPythonPath());
		gridData = new GridData();
		gridData.horizontalSpan = 3;
		gridData = new GridData();
		gridData.horizontalSpan = 3;
		gridData.verticalIndent = 20;
		m_info = new Label(this, SWT.NONE);
		m_info.setLayoutData(gridData);
		gridData = new GridData();
		gridData.horizontalSpan = 3;
		m_error = new Label(this, SWT.NONE);
		m_error.setLayoutData(gridData);
		final Color red = new Color(parent.getDisplay(), 255, 0, 0);
		m_error.setForeground(red);
		m_error.addDisposeListener(new DisposeListener() {
			@Override
			public void widgetDisposed(DisposeEvent e) {
				red.dispose();
			}
		});
	}
	
	public void setPythonPath(final String pythonPath) {
		m_pathEditor.setStringValue(pythonPath);
	}
	
	public String getPythonPath() {
		return m_pathEditor.getStringValue();
	}
	
	public void setInfo(final String info) {
		m_info.setText(info);
		refreshSizes();
	}
	
	public void setError(final String error) {
		m_error.setText(error);
		refreshSizes();
	}

	/**
	 * Refreshes the pages layout and size.
	 */
	private void refreshSizes() {
		layout();
	}

}
