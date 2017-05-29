package org.knime.python2.nodes.view;

import java.awt.Image;

import javax.swing.ImageIcon;
import javax.swing.JLabel;

import org.knime.core.node.NodeView;

public class PythonViewNodeView extends NodeView<PythonViewNodeModel> {
	
	private JLabel m_label = new JLabel();
	
	public PythonViewNodeView(final PythonViewNodeModel model) {
		super(model);
		super.setComponent(m_label);
	}

	@Override
	protected void onClose() {
		// nothing
	}

	@Override
	protected void onOpen() {
		// nothing
	}

	@Override
	protected void modelChanged() {
		Image image = getNodeModel().getOutputImage();
		if (image != null) {
			m_label.setText(null);
			m_label.setIcon(new ImageIcon(image));
		} else {
			m_label.setText("No image");
			m_label.setIcon(null);
		}
	}

}
