/* @(#)$RCSfile$
 * $Revision$ $Date$ $Author$
 *
 */
package org.knime.ext.jython;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "JPython Function Node" Node. Scripting Engine
 *
 * @author Tripos
 */
public class PythonFunctionNodeFactory extends NodeFactory<PythonFunctionNodeModel>
{
	/**
	 * {@inheritDoc}
	 */
	public PythonFunctionNodeModel createNodeModel()
	{
		return new PythonFunctionNodeModel();
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
	public NodeView<PythonFunctionNodeModel> createNodeView(final int viewIndex,
			final PythonFunctionNodeModel nodeModel)
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
		return new PythonFunctionNodeDialog();
	}

}
