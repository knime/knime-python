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

    private static final String CFG_KEY_PACKAGES = "included_packages";

    private static final String CFG_KEY_PACKAGE_NAMES = "names";

    private static final String CFG_KEY_PACKAGE_VERSIONS = "versions";

    private static final String CFG_KEY_PACKAGE_BUILDS = "builds";

    private static final String CFG_KEY_PACKAGE_CHANNELS = "channels";

    private List<CondaPackageSpec> m_packages = Collections.emptyList();

    public List<CondaPackageSpec> getPackages() {
        return Collections.unmodifiableList(m_packages);
    }

    public void setPackages(final List<CondaPackageSpec> packages) {
        m_packages = new ArrayList<>(packages);
    }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        final int numPackages = m_packages.size();
        final String[] names = new String[numPackages];
        final String[] versions = new String[numPackages];
        final String[] builds = new String[numPackages];
        final String[] channels = new String[numPackages];
        for (int i = 0; i < numPackages; i++) {
            final CondaPackageSpec pkg = m_packages.get(i);
            names[i] = pkg.getName();
            versions[i] = pkg.getVersion();
            builds[i] = pkg.getBuild();
            channels[i] = pkg.getChannel();
        }
        final NodeSettingsWO subSettings = settings.addNodeSettings(CFG_KEY_PACKAGES);
        subSettings.addStringArray(CFG_KEY_PACKAGE_NAMES, names);
        subSettings.addStringArray(CFG_KEY_PACKAGE_VERSIONS, versions);
        subSettings.addStringArray(CFG_KEY_PACKAGE_BUILDS, builds);
        subSettings.addStringArray(CFG_KEY_PACKAGE_CHANNELS, channels);
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
        final String[] names = subSettings.getStringArray(CFG_KEY_PACKAGE_NAMES);
        final String[] versions = subSettings.getStringArray(CFG_KEY_PACKAGE_VERSIONS);
        final String[] builds = subSettings.getStringArray(CFG_KEY_PACKAGE_BUILDS);
        final String[] channels = subSettings.getStringArray(CFG_KEY_PACKAGE_CHANNELS);
        final int numPackages = names.length;
        if (!(versions.length == numPackages && builds.length == numPackages && channels.length == numPackages)) {
            throw new InvalidSettingsException(
                "The arrays containing the individual parts of the package specifications must be of equal length.");
        }
        final List<CondaPackageSpec> packages = new ArrayList<>(numPackages);
        for (int i = 0; i < numPackages; i++) {
            packages.add(new CondaPackageSpec(names[i], versions[i], builds[i], channels[i]));
        }
        return packages;
    }
}
