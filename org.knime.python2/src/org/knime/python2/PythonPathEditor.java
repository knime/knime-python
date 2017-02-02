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
