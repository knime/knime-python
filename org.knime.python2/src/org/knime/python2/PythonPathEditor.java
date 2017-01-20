package org.knime.python2;

import org.eclipse.jface.preference.FileFieldEditor;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;

public class PythonPathEditor extends Composite {
	
	private FileFieldEditor m_pathEditor;
	private Label m_info;
	private Label m_error;
	private ScrolledComposite m_sc;
	Composite m_container;

	public PythonPathEditor(final String label, final Composite parent) {
		super(parent, SWT.NONE);
		m_sc = new ScrolledComposite(parent, SWT.H_SCROLL | SWT.V_SCROLL);
		m_container = new Composite(m_sc, SWT.NONE);
		m_container.setLayout(new GridLayout());
		m_pathEditor = new FileFieldEditor(Activator.PLUGIN_ID, label, m_container);
		m_pathEditor.setStringValue(getPythonPath());
		GridData gridData = new GridData();
		gridData.horizontalSpan = 3;
		gridData = new GridData();
		gridData.horizontalSpan = 3;
		gridData.verticalIndent = 20;
		m_info = new Label(m_container, SWT.NONE);
		m_info.setLayoutData(gridData);
		gridData = new GridData();
		gridData.horizontalSpan = 3;
		m_error = new Label(m_container, SWT.NONE);
		m_error.setLayoutData(gridData);
		final Color red = new Color(parent.getDisplay(), 255, 0, 0);
		m_error.setForeground(red);
		m_error.addDisposeListener(new DisposeListener() {
			@Override
			public void widgetDisposed(DisposeEvent e) {
				red.dispose();
			}
		});
		m_sc.setContent(m_container);
		m_sc.setExpandHorizontal(true);
		m_sc.setExpandVertical(true);
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
		m_container.layout();
		m_sc.setMinSize(m_container.computeSize(SWT.DEFAULT, SWT.DEFAULT));
	}

}
