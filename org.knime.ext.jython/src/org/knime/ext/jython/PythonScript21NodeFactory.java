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
 * <code>NodeFactory</code> for the "ScriptedNode" Node. Scripting Engine
 *
 * @author Tripos
 */
public class PythonScript21NodeFactory extends NodeFactory
{
	/**
	 * @see org.knime.core.node.NodeFactory#createNodeModel()
	 */
	public NodeModel createNodeModel()
	{
		return new PythonScriptNodeModel(2, 1);
	}

	/**
	 * @see org.knime.core.node.NodeFactory#getNrNodeViews()
	 */
	public int getNrNodeViews()
	{
		return 0;
	}

	/**
	 * @see org.knime.core.node.NodeFactory#createNodeView(int,
	 *      org.knime.core.node.NodeModel)
	 */
	public NodeView createNodeView(final int viewIndex,
			final NodeModel nodeModel)
	{
		return null;
	}

	/**
	 * @see org.knime.core.node.NodeFactory#hasDialog()
	 */
	public boolean hasDialog()
	{
		return true;
	}

	/**
	 * @see org.knime.core.node.NodeFactory#createNodeDialogPane()
	 */
	public NodeDialogPane createNodeDialogPane()
	{
		return new PythonScriptNodeDialog();
	}

}
