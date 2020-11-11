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
 *   Nov 5, 2020 (marcel): created
 */
package org.knime.python2.nodes.conda;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.python2.CondaPackageSpec;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class CondaPackagesConfig {

    private static final String CFG_KEY_PACKAGES = "packages";

    private static final String CFG_KEY_NUM_PACKAGES = "number_packages";

    private static final String CFG_KEY_PACKAGE_NAME = "name";

    private static final String CFG_KEY_PACKAGE_VERSION = "version";

    private static final String CFG_KEY_PACKAGE_BUILD = "build";

    private static final String CFG_KEY_PACKAGE_CHANNEL = "channel";

    private List<CondaPackageSpec> m_packages = Collections.emptyList();

    public List<CondaPackageSpec> getPackages() {
        return Collections.unmodifiableList(m_packages);
    }

    public void setPackages(final List<CondaPackageSpec> packages) {
        m_packages = new ArrayList<>(packages);
    }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        final List<CondaPackageSpec> packages = m_packages;
        final NodeSettingsWO subSettings = settings.addNodeSettings(CFG_KEY_PACKAGES);
        subSettings.addInt(CFG_KEY_NUM_PACKAGES, packages.size());
        for (int i = 0; i < packages.size(); i++) {
            final NodeSettingsWO packageSettings = subSettings.addNodeSettings(Integer.toString(i));
            final CondaPackageSpec pkg = packages.get(i);
            packageSettings.addString(CFG_KEY_PACKAGE_NAME, pkg.getName());
            packageSettings.addString(CFG_KEY_PACKAGE_VERSION, pkg.getVersion());
            packageSettings.addString(CFG_KEY_PACKAGE_BUILD, pkg.getBuild());
            packageSettings.addString(CFG_KEY_PACKAGE_CHANNEL, pkg.getChannel());
        }
    }

    public static void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        readPackagesFrom(settings);
    }

    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_packages = readPackagesFrom(settings);
    }

    private static List<CondaPackageSpec> readPackagesFrom(final NodeSettingsRO settings)
        throws InvalidSettingsException {
        final NodeSettingsRO subSettings = settings.getNodeSettings(CFG_KEY_PACKAGES);
        final int numPackages = subSettings.getInt(CFG_KEY_NUM_PACKAGES);
        final List<CondaPackageSpec> packages = new ArrayList<>(numPackages);
        for (int i = 0; i < numPackages; i++) {
            final NodeSettingsRO packageSettings = subSettings.getNodeSettings(Integer.toString(i));
            final String name = packageSettings.getString(CFG_KEY_PACKAGE_NAME);
            final String version = packageSettings.getString(CFG_KEY_PACKAGE_VERSION);
            final String build = packageSettings.getString(CFG_KEY_PACKAGE_BUILD);
            final String channel = packageSettings.getString(CFG_KEY_PACKAGE_CHANNEL);
            packages.add(new CondaPackageSpec(name, version, build, channel));
        }
        return packages;
    }
}
