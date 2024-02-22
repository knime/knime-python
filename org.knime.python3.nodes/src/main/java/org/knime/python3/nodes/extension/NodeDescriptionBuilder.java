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

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.xmlbeans.XmlException;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDescription41Proxy;
import org.knime.core.node.NodeLogger;
import org.knime.node.v41.NodeType;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * A builder for {@link NodeDescription NodeDescriptions}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class NodeDescriptionBuilder {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(NodeDescriptionBuilder.class);

    private final String m_name;

    private Path m_iconPath;

    private String m_shortDescription = "";

    private String m_intro = "";

    private final String m_nodeType;

    private final List<Port> m_inputPorts = new ArrayList<>();

    private final List<Port> m_outputPorts = new ArrayList<>();

    private final List<DynamicPort> m_dynamicInputPorts = new ArrayList<>();

    private final List<DynamicPort> m_dynamicOutputPorts = new ArrayList<>();

    private final List<DynamicPort> m_dynamicInputPortTypes = new ArrayList<>();

    private final List<DynamicPort> m_dynamicOutputPortTypes = new ArrayList<>();

    private final List<Option> m_topLevelOptions = new ArrayList<>();

    private final List<Tab> m_tabs = new ArrayList<>();

    private final List<View> m_views = new ArrayList<>();

    private final List<String> m_keywords = new ArrayList<>();

    private final boolean m_isDeprecated;

    /**
     * Constructor for a new builder.
     *
     * @param name of the node
     * @param nodeType of the node
     */
    public NodeDescriptionBuilder(final String name, final String nodeType, final boolean isDeprecated) {
        m_name = name;
        // will complain if nodeType is not a valid NodeType
        NodeType.Enum.forString(nodeType);
        m_nodeType = nodeType;
        m_isDeprecated = isDeprecated;
        // m_dynamicInputPorts = [];
    }

    /**
     * @return the NodeDescription
     */
    public NodeDescription build() {

        var fac = NodeDescription.getDocumentBuilderFactory();

        DocumentBuilder docBuilder = createDocBuilder(fac);

        // create Document for Node
        var doc = docBuilder.newDocument();

        var buildHelper = new BuildHelper(docBuilder, doc);

        var node = doc.createElement("knimeNode");

        node.setAttribute("icon", m_iconPath.toAbsolutePath().toString());
        node.setAttribute("type", m_nodeType);
        node.setAttribute("deprecated", Boolean.toString(m_isDeprecated));
        var name = doc.createElement("name");
        name.setTextContent(m_name);
        node.appendChild(name);

        // short description
        var shortDesc = doc.createElement("shortDescription");
        shortDesc.appendChild(buildHelper.parseDocumentFragment(getShortDescription()));
        node.appendChild(shortDesc);

        // intro
        var fullDesc = doc.createElement("fullDescription");
        var intro = doc.createElement("intro");
        intro.appendChild(buildHelper.parseDocumentFragment(getIntro()));
        fullDesc.appendChild(intro);
        node.appendChild(fullDesc);

        // tabs
        for (var tab : m_tabs) {
            var tabDesc = doc.createElement("tab");
            tabDesc.setAttribute("name", tab.getName());
            var description = doc.createElement("description");
            // only add a description to the tab if it's been provided
            var desc = tab.getDescription();
            if (desc != null && !desc.isBlank()) {
                description.appendChild(buildHelper.parseDocumentFragment(tab.getDescription()));
                tabDesc.appendChild(description);
            }
            for (var option : tab.m_options) {
                tabDesc.appendChild(buildHelper.createOptionElement(option));
            }
            fullDesc.appendChild(tabDesc);
        }

        // options
        for (var option : m_topLevelOptions) {
            fullDesc.appendChild(buildHelper.createOptionElement(option));
        }

        // create ports
        // NOTE:
        // We always need the "ports" element even if there are no ports.
        // Otherwise the XML validation will fail.
        var ports = doc.createElement("ports");
        buildHelper.createElements("inPort", m_inputPorts).forEach(ports::appendChild);
        buildHelper.createElements("outPort", m_outputPorts).forEach(ports::appendChild);

        // TODO: HERE BROKNNNNN
        if (!m_dynamicInputPorts.isEmpty()) {
            int i = 0;
            for (DynamicPort port : m_dynamicInputPorts) {
                var dynamicInputPorts = doc.createElement("dynInPort");

                // node description and factory contain different (extendable) input port group identfier
                String nameString = port.getName();
                String typeString = "";
                for (DynamicPort portType : m_dynamicInputPortTypes) {
                    if(portType.getName().equals(nameString)) {
                        typeString = portType.getDescription();
                    }
                }

                dynamicInputPorts.setAttribute("name", nameString);
                dynamicInputPorts.setAttribute("group-identifier", String.format("Input %s # %d",typeString, i)); //TODO: use
                dynamicInputPorts.setAttribute("insert-before", "0");

                var portDescription = buildHelper.parseDocumentFragment(port.getDescription());
                dynamicInputPorts.appendChild(portDescription);

                ports.appendChild(dynamicInputPorts);
                i += 1;
            }
        }

        if (!m_dynamicOutputPorts.isEmpty()) {
            int i = 0;
            for (DynamicPort port : m_dynamicOutputPorts) {
                var dynamicOutputPorts = doc.createElement("dynOutPort");

             // node description and factory contain different (extendable) input port group identfier
                String nameString = port.getName();
                String typeString = "";
                for (DynamicPort portType : m_dynamicOutputPortTypes) {
                    if(portType.getName().equals(nameString)) {
                        typeString = portType.getDescription();
                    }
                }


                dynamicOutputPorts.setAttribute("name", port.getName());
                dynamicOutputPorts.setAttribute("group-identifier", String.format("Output %s # %d",typeString, i));
                dynamicOutputPorts.setAttribute("insert-before", "0");

                var portDescription = buildHelper.parseDocumentFragment(port.getDescription());
                dynamicOutputPorts.appendChild(portDescription);

                ports.appendChild(dynamicOutputPorts);
                i += 1;
            }
        }

        node.appendChild(ports);

        if (!m_views.isEmpty()) {
            var views = doc.createElement("views");
            buildHelper.createElements("view", m_views).forEach(views::appendChild);
            node.appendChild(views);
        }

        if (!m_keywords.isEmpty()) {
            var keywords = doc.createElement("keywords");
            for (String keyword : m_keywords) {
                var child = doc.createElement("keyword");
                child.setTextContent(keyword);
                keywords.appendChild(child);
            }
            node.appendChild(keywords);
        }


        doc.appendChild(node);

        try {
            return new NodeDescription41Proxy(doc);
        } catch (XmlException e) {
            // should never happen
            throw new IllegalStateException("Problem creating node description", e);
        }

    }

    private static final class BuildHelper {

        private final DocumentBuilder m_docBuilder;

        private final Document m_doc;

        BuildHelper(final DocumentBuilder docBuilder, final Document doc) {
            m_docBuilder = docBuilder;
            m_doc = doc;
        }

        Stream<Element> createElements(final String elementType, final List<? extends Described> describeds) {
            return IntStream.range(0, describeds.size())//
                .mapToObj(i -> createIndexedDescribed(elementType, describeds.get(i), i));
        }

        Element createIndexedDescribed(final String elementType, final Described described, final int idx) {
            var element = createDescribed(elementType, described);
            element.setAttribute("index", Integer.toString(idx));
            return element;
        }

        Element createOptionElement(final Option option) {
            return createDescribed("option", option);
        }

        Element createDescribed(final String elementType, final Described described) {
            var element = m_doc.createElement(elementType);
            element.setAttribute("name", described.getName());
            element.appendChild(parseDocumentFragment(described.getDescription()));
            return element;
        }

        private DocumentFragment parseDocumentFragment(final String s) {
            var wrapped = "<fragment>" + s + "</fragment>";
            Document parsed;
            try {
                parsed = m_docBuilder.parse(new InputSource(new StringReader(wrapped)));
            } catch (SAXException | IOException e) {
                // should never happen
                throw new IllegalStateException("Problem creating node description", e);
            }
            var fragment = m_doc.createDocumentFragment();
            var children = parsed.getDocumentElement().getChildNodes();
            for (var i = 0; i < children.getLength(); i++) {
                var child = m_doc.importNode(children.item(i), true);
                fragment.appendChild(child);
            }
            return fragment;
        }
    }

    private static DocumentBuilder createDocBuilder(final DocumentBuilderFactory fac) {
        try {
            return fac.newDocumentBuilder(); //NOSONAR
        } catch (ParserConfigurationException e) {
            // should never happen
            throw new IllegalStateException("Problem creating node description", e);
        }
    }

    private String getIntro() {
        if (m_intro.isBlank()) {
            LOGGER.codingWithFormat("Please provide an intro for the node %s.", m_name);
            return m_name;

        } else {
            return m_intro;
        }
    }

    private String getShortDescription() {
        if (m_shortDescription.isBlank()) {
            LOGGER.codingWithFormat("Please provide a short description for the node %s.", m_name);
            return m_name;
        } else {
            return m_shortDescription;
        }
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
     * @param name of the input port
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
     * @param name of the port
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
     * @param name of the option
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
     * Adds a keyword to xml.
     *
     * @param keywords a list of keywords
     * @return this builder
     */
    public NodeDescriptionBuilder withKeywords(final String[] keywords) {
        Collections.addAll(m_keywords, keywords);
        return this;
    }

    /**
     * Adds a Dynamic Input Port.
     *
     * @param name of the Port
     * @param description of the Port
     * @return this builder
    */
    public NodeDescriptionBuilder withDynamicInputPorts(final String name, final String description) {
        DynamicPort dynamicPort = new DynamicPort(name, description);
        m_dynamicInputPorts.add(dynamicPort);
        return this;
    }
    /**
     * Adds a Dynamic Output Port.
     *
     * @param name of the Port
     * @param description of the Port
     * @return this builder
     */
    public NodeDescriptionBuilder withDynamicOuputPorts(final String name, final String description) {
        DynamicPort dynamicPort = new DynamicPort(name, description);
        m_dynamicOutputPorts.add(dynamicPort);
        return this;
    }
    /**
     * @param name
     * @param type
     * @return
     */
    public NodeDescriptionBuilder withDynamicInputPortTypes(final String name, final String description) {
        DynamicPort dynamicPort = new DynamicPort(name, description);
        m_dynamicInputPortTypes.add(dynamicPort);
        return this;
    }
    /**
     * @param name
     * @param type
     * @return
     */
    public NodeDescriptionBuilder withDynamicOutputPortTypes(final String name, final String description) {
        DynamicPort dynamicPort = new DynamicPort(name, description);
        m_dynamicOutputPortTypes.add(dynamicPort);
        return this;
    }


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

    private static final class DynamicPort extends Described {
        private String m_portType;

        DynamicPort(final String name, final String description) {
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
         * @param name of the tab
         * @param description of the tab
         * @return the builder
         */
        public static Builder builder(final String name, final String description) {
            return new Builder(name, description);
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
    }

    private static final class View extends Described {

        View(final String name, final String description) {
            super(name, description);
        }
    }

}
