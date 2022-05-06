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
 *   Apr 28, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.nodes.extension;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDescription41Proxy;
import org.knime.node.v41.KnimeNodeDocument;
import org.knime.node.v41.NodeType;
import org.knime.node.v41.Views;

/**
 * A builder for {@link NodeDescription NodeDescriptions}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class NodeDescriptionBuilder {

    private final String m_name;

    private final NodeType.Enum m_nodeType;

    private Path m_iconPath;

    private String m_shortDescription = "";

    private String m_intro = "";

    private final List<Port> m_inputPorts = new ArrayList<>();

    private final List<Port> m_outputPorts = new ArrayList<>();

    private final List<Option> m_topLevelOptions = new ArrayList<>();

    private final List<Tab> m_tabs = new ArrayList<>();

    private final List<View> m_views = new ArrayList<>();

    /**
     * Constructor for a new builder.
     *
     * @param name     of the node
     * @param nodeType of the node
     */
    public NodeDescriptionBuilder(final String name, final String nodeType) {
        m_name = name;
        m_nodeType = NodeType.Enum.forString(nodeType);
    }

    /**
     * @return the NodeDescription
     */
    public NodeDescription build() {
        var doc = KnimeNodeDocument.Factory.newInstance();
        var node = doc.addNewKnimeNode();
        node.setName(m_name);
        node.setType(m_nodeType);
        if (m_iconPath != null) {
            node.setIcon(m_iconPath.toAbsolutePath().toString());
        }

        if (!m_shortDescription.isBlank()) {
            node.setShortDescription(m_shortDescription);
        }

        var fullDescription = node.addNewFullDescription();
        if (!m_intro.isBlank()) {
            fullDescription.addNewIntro().addNewP().newCursor().setTextValue(m_intro);
        }

        m_topLevelOptions.forEach(o -> o.fill(fullDescription.addNewOption()));

        m_tabs.forEach(t -> t.fill(fullDescription.addNewTab()));

        var ports = node.addNewPorts();
        if (!(m_inputPorts.isEmpty() && m_outputPorts.isEmpty())) {
            int inputIdx = 0;// NOSONAR
            for (var inPort : m_inputPorts) {
                var port = ports.addNewInPort();
                port.setIndex(BigInteger.valueOf(inputIdx));
                inputIdx++;
                port.setName(inPort.getName());
                port.addNewP().newCursor().setTextValue(inPort.getDescription());
            }
            int outputIdx = 0;// NOSONAR
            for (var outPort : m_outputPorts) {
                var port = ports.addNewOutPort();
                port.setIndex(BigInteger.valueOf(outputIdx));
                outputIdx++;
                port.setName(outPort.getName());
                port.addNewP().newCursor().setTextValue(outPort.getDescription());
            }
        } else {
            throw new IllegalStateException("Node " + m_name + " has no input or output ports configured");
        }

        if (!m_views.isEmpty()) {
            final Views views = node.addNewViews();
            for (int i = 0; i < m_views.size(); i++) {
                final View view = m_views.get(i);
                org.knime.node.v41.View viewXML = views.addNewView();
                viewXML.setName(view.getName());
                viewXML.setIndex(BigInteger.valueOf(i));
                viewXML.addNewP().newCursor().setTextValue(view.getDescription());
            }
        }

        return new NodeDescription41Proxy(doc);
    }

    /**
     * Sets a new icon.
     *
     * @param iconPath the path to the icon
     * @return this builder
     */
    public NodeDescriptionBuilder withIcon(final Path iconPath) {
        m_iconPath = iconPath;
        return this;
    }

    /**
     * Sets a new short description.
     *
     * @param shortDescription of the node
     * @return this builder
     */
    public NodeDescriptionBuilder withShortDescription(final String shortDescription) {
        m_shortDescription = shortDescription;
        return this;
    }

    /**
     * Sets a new intro.
     *
     * @param intro of the node description
     * @return this builder
     */
    public NodeDescriptionBuilder withIntro(final String intro) {
        m_intro = intro;
        return this;
    }

    /**
     * Adds an input port.
     *
     * @param name        of the input port
     * @param description of the input port
     * @return this builder
     */
    public NodeDescriptionBuilder withInputPort(final String name, final String description) {
        m_inputPorts.add(new Port(name, description));
        return this;
    }

    /**
     * Adds an output port.
     *
     * @param name        of the port
     * @param description of the port
     * @return this builder
     */
    public NodeDescriptionBuilder withOutputPort(final String name, final String description) {
        m_outputPorts.add(new Port(name, description));
        return this;
    }

    /**
     * Adds a top-level option.
     *
     * @param name        of the option
     * @param description of the option
     * @return this builder
     */
    public NodeDescriptionBuilder withOption(final String name, final String description) {
        m_topLevelOptions.add(new Option(name, description));
        return this;
    }

    /**
     * Adds a tab.
     *
     * @param tab to add (use {@link Tab.Builder} to create one)
     * @return this builder
     */
    public NodeDescriptionBuilder withTab(final Tab tab) {
        m_tabs.add(tab);
        return this;
    }

    /**
     * Adds a view.
     *
     * @param name        of the view
     * @param description of the view
     * @return this builder
     */
    public NodeDescriptionBuilder withView(final String name, final String description) {
        m_views.add(new View(name, description));
        return this;
    }

    private abstract static class Described { // NOSONAR will become obsolete once we can use records in Java 17

        private final String m_name;

        private final String m_description;

        protected Described(final String name, final String description) {
            m_name = name;
            m_description = description;
        }

        protected String getName() {
            return m_name;
        }

        protected String getDescription() {
            return m_description;
        }
    }

    private static final class Port extends Described {
        Port(final String name, final String description) {
            super(name, description);
        }

    }

    /**
     * Represents a Tab in the NodeDescription.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public static final class Tab extends Described {

        private final List<Option> m_options;

        private Tab(final Builder builder) {
            super(builder.getName(), builder.getDescription());
            m_options = new ArrayList<>(builder.m_options);
        }

        /**
         * Creates a Builder for a Tab.
         *
         * @param name        of the tab
         * @param description of the tab
         * @return the builder
         */
        public static Builder builder(final String name, final String description) {
            return new Builder(name, description);
        }

        private void fill(final org.knime.node.v41.Tab tab) {
            tab.setName(getName());
            tab.addNewDescription().newCursor().setTextValue(getDescription());
            for (var option : m_options) {
                option.fill(tab.addNewOption());
            }
        }

        /**
         * A builder for tabs.
         *
         * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
         */
        public static final class Builder extends Described {

            private final List<Option> m_options = new ArrayList<>();

            private Builder(final String name, final String description) {
                super(name, description);
            }

            /**
             * Adds an option to the tab.
             *
             * @param name        of the option
             * @param description of the option
             * @return this builder
             */
            public Builder withOption(final String name, final String description) {
                m_options.add(new Option(name, description));
                return this;
            }

            /**
             * @return the tab
             */
            public Tab build() {
                return new Tab(this);
            }

        }
    }

    private static final class Option extends Described {

        Option(final String name, final String description) {
            super(name, description);
        }

        void fill(final org.knime.node.v41.Option option) {
            option.setName(getName());
            option.addNewP().newCursor().setTextValue(getDescription());
        }
    }

    private static final class View extends Described {

        View(final String name, final String description) {
            super(name, description);
        }
    }

}