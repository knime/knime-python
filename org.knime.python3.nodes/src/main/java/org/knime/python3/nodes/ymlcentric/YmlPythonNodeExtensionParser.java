package org.knime.python3.nodes.ymlcentric;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.knime.python3.nodes.PurePythonNodeSetFactory.PythonExtensionParser;
import org.knime.python3.nodes.PythonNode;
import org.yaml.snakeyaml.Yaml;

/**
 * Reads a PythonNodeExtension from a yml file.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class YmlPythonNodeExtensionParser implements PythonExtensionParser {

    @Override
    public PythonNodeExtension parseExtension(final Path pathToExtension) throws IOException {
        var yaml = new Yaml();
        try (var inputStream = Files.newInputStream(pathToExtension.resolve("knime.yml"))) {
            Map<String, Object> obj = yaml.load(inputStream);
            return parse(obj);
        }
    }

    private static PythonNodeExtension parse(final Map<String, Object> extensionObject) {
        var groupId = (String)extensionObject.get("group_id");
        var bundleName = (String)extensionObject.get("name");
        var id = groupId + "." + bundleName;
        var description = (String)extensionObject.get("description");
        var author = (String)extensionObject.get("author");
        var environmentName = (String)extensionObject.get("environment_name");
        var factoryModule = (String)extensionObject.get("factory_module");
        var factoryMethod = (String)extensionObject.get("factory_method");
        @SuppressWarnings("unchecked")
        var nodeList = (List<Map<String, Object>>)extensionObject.get("nodes");
        var nodes = nodeList.stream()//
                .map(YmlPythonNodeExtensionParser::parseNode)//
                .toArray(PythonNode[]::new);
        return new PythonNodeExtension(id, description, author, environmentName, factoryModule, factoryMethod, nodes);
    }

    private static PythonNode parseNode(final Map<String, Object> nodeObject) {
        var id = (String)nodeObject.get("id");
        var categoryPath = (String)nodeObject.get("category_path");
        var afterId = (String)nodeObject.get("after_id");
        var name = (String)nodeObject.get("name");
        var iconPath = (String)nodeObject.get("icon");
        var type = (String)nodeObject.get("type");
        return new PythonNode(id, categoryPath, afterId, iconPath, null);
    }

}
