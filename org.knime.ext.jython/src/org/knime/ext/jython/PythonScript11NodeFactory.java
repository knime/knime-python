/* @(#)$RCSfile$
 * $Revision$ $Date$ $Author$
 *
 */
package org.knime.ext.jython;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "JPython Script 1:1" Node. Scripting Engine
 *
 * @author Tripos
 */
public class PythonScript11NodeFactory extends NodeFactory
{
	/**
	 * {@inheritDoc}
	 */
	public NodeModel createNodeModel()
	{
		return new PythonScriptNodeModel(1, 1);
	}

	/**
	 * {@inheritDoc}
	 */
	public int getNrNodeViews()
	{
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	public NodeView createNodeView(final int viewIndex,
			final NodeModel nodeModel)
	{
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean hasDialog()
	{
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	public NodeDialogPane createNodeDialogPane()
	{
		return new PythonScriptNodeDialog();
	}

}
