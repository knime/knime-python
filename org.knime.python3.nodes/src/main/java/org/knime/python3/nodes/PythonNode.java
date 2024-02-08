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
 *   Feb 22, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes;

import org.knime.core.node.NodeDescription;
import org.knime.core.node.port.PortType;
import org.knime.python3.nodes.extension.ExtensionNode;
import org.knime.python3.nodes.ports.PythonPortObjects;
import org.knime.python3.views.ViewResources;

/**
 * Represents a PythonNode.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class PythonNode implements ExtensionNode {

    private final String m_id;

    private final String m_categoryPath;

    private final String m_afterId;

    private final String[] m_keywords;

    private final PortType[] m_inputPortTypesGroups;

    private final PortType[] m_outputPortTypesGroups;

    private final PortType[] m_inputPortTypes;

    private final PortType[] m_outputPortTypes;

    private final int m_numViews;

    private final NodeDescription m_description;

    private final boolean m_isDeprecated;

    private final boolean m_isHidden;

    private final ViewResources[] m_viewResources;


    /**
     * Constructor.
     *
     * @param id of the node
     * @param categoryPath path to the category the node is contained in in the node repository
     * @param afterId id of the node after which to insert this node
     * @param keywords
     * @param description the node's description
     * @param inputPortTypes
     * @param outputPortTypes
     * @param numViews
     * @param isDeprecated whether the node is deprecated
     * @param isHidden whether the node is hidden from node repository
     * @param viewResources
     */
    public PythonNode(
        final String id, //
        final String categoryPath, //
        final String afterId, //
        final String[] keywords, //
        final NodeDescription description, //
        final String[] inputPortTypes, //
        final String[] outputPortTypes, //
        final int numViews, //
        final boolean isDeprecated, //
        final boolean isHidden, //
        final ViewResources[] viewResources) {
        m_id = id;
        m_categoryPath = categoryPath;
        m_afterId = afterId;
        m_keywords = keywords;
        m_description = description;

        m_inputPortTypesGroups = PythonPortObjects.getGroupPortTypesForIdentifiers(inputPortTypes);
        m_outputPortTypesGroups = PythonPortObjects.getGroupPortTypesForIdentifiers(outputPortTypes);

        m_inputPortTypes = PythonPortObjects.getPortTypesForIdentifiers(inputPortTypes);
        m_outputPortTypes = PythonPortObjects.getPortTypesForIdentifiers(outputPortTypes);
        m_numViews = numViews;
        m_isDeprecated = isDeprecated;
        m_isHidden = isHidden;
        m_viewResources = viewResources;
    }

    /**
     * @return id of the node
     */
    @Override
    public String getId() {
        return m_id;
    }

    /**
     * @return category path
     */
    @Override
    public String getCategoryPath() {
        return m_categoryPath;
    }

    /**
     * @return id of the node after which to insert this node
     */
    @Override
    public String getAfterId() {
        return m_afterId;
    }

    /**
     * @return keywords
     */
    @Override
    public String[] getKeywords() {
        return m_keywords;
    }

    /**
     * @return the nodes description
     */
    @Override
    public NodeDescription getNodeDescription() {
        return m_description;
    }

    /**
     * @return Input port type identifiers
     */
    @Override
    public PortType[] getInputPortTypesGroups() {
        return m_inputPortTypesGroups;
    }

    /**
     * @return Output port type identifiers
     */
    @Override
    public PortType[] getOutputPortTypesGroups() {
        return m_outputPortTypesGroups;
    }


    /**
     * @return Input port type identifiers
     */
    @Override
    public PortType[] getInputPortTypes() {
        return m_inputPortTypes;
    }

    /**
     * @return Output port type identifiers
     */
    @Override
    public PortType[] getOutputPortTypes() {
        return m_outputPortTypes;
    }

    /**
     * @return The number of views offered by this node
     */
    @Override
    public int getNumViews() {
        return m_numViews;
    }

    @Override
    public boolean isDeprecated() {
        return m_isDeprecated;
    }

    @Override
    public boolean isHidden() {
        return m_isHidden;
    }

    @Override
    public ViewResources[] getViewResources() {
        return m_viewResources;
    }
}