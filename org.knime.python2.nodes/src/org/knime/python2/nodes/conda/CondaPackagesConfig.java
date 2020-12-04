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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.python2.conda.CondaPackageSpec;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class CondaPackagesConfig {

    private static final String CFG_KEY_INCLUDED_PACKAGES = "included_packages";

    private static final String CFG_KEY_EXCLUDED_PACKAGES = "excluded_packages";

    private static final String CFG_KEY_PACKAGE_NAMES = "names";

    private static final String CFG_KEY_PACKAGE_VERSIONS = "versions";

    private static final String CFG_KEY_PACKAGE_BUILDS = "builds";

    private static final String CFG_KEY_PACKAGE_CHANNELS = "channels";

    private List<CondaPackageSpec> m_included = Collections.emptyList();

    private List<CondaPackageSpec> m_excluded = Collections.emptyList();

    public List<CondaPackageSpec> getIncludedPackages() {
        return Collections.unmodifiableList(m_included);
    }

    public List<CondaPackageSpec> getExcludedPackages() {
        return Collections.unmodifiableList(m_excluded);
    }

    public void setPackages(final List<CondaPackageSpec> included, final List<CondaPackageSpec> excluded) {
        m_included = new ArrayList<>(included);
        m_excluded = new ArrayList<>(excluded);
    }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        writePackagesTo(m_included, settings.addNodeSettings(CFG_KEY_INCLUDED_PACKAGES));
        writePackagesTo(m_excluded, settings.addNodeSettings(CFG_KEY_EXCLUDED_PACKAGES));
    }

    private static void writePackagesTo(final List<CondaPackageSpec> packages, final NodeSettingsWO subSettings) {
        final int numPackages = packages.size();
        final String[] names = new String[numPackages];
        final String[] versions = new String[numPackages];
        final String[] builds = new String[numPackages];
        final String[] channels = new String[numPackages];
        for (int i = 0; i < numPackages; i++) {
            final CondaPackageSpec pkg = packages.get(i);
            names[i] = pkg.getName();
            versions[i] = pkg.getVersion();
            builds[i] = pkg.getBuild();
            channels[i] = pkg.getChannel();
        }
        subSettings.addStringArray(CFG_KEY_PACKAGE_NAMES, names);
        subSettings.addStringArray(CFG_KEY_PACKAGE_VERSIONS, versions);
        subSettings.addStringArray(CFG_KEY_PACKAGE_BUILDS, builds);
        subSettings.addStringArray(CFG_KEY_PACKAGE_CHANNELS, channels);
    }

    public static void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final List<CondaPackageSpec> included = readPackagesFrom(settings.getNodeSettings(CFG_KEY_INCLUDED_PACKAGES));
        final List<String> duplicateNames = included.stream() //
            .collect(Collectors.groupingBy(CondaPackageSpec::getName, LinkedHashMap::new, Collectors.counting())) //
            .entrySet().stream() //
            .filter(m -> m.getValue() > 1) //
            .map(Map.Entry::getKey) //
            .collect(Collectors.toList());
        if (!duplicateNames.isEmpty()) {
            throw new InvalidSettingsException(
                "The same package cannot be selected for inclusion multiple times. Package(s): "
                    + String.join(", ", duplicateNames) + ".");
        }
        final List<CondaPackageSpec> excluded = readExcludedPackages(settings);
        final Set<CondaPackageSpec> intersection = new HashSet<>(included);
        intersection.retainAll(excluded);
        if (!intersection.isEmpty()) {
            throw new InvalidSettingsException(
                "A package cannot be both included and excluded. Violating package(s): " + intersection);
        }
    }

    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_included = readPackagesFrom(settings.getNodeSettings(CFG_KEY_INCLUDED_PACKAGES));
        m_excluded = readExcludedPackages(settings);
    }

    /**
     * Backward compatibility: KNIME 4.3 did not maintain a list of excluded packages.
     */
    private static List<CondaPackageSpec> readExcludedPackages(final NodeSettingsRO settings)
        throws InvalidSettingsException {
        return settings.containsKey(CFG_KEY_EXCLUDED_PACKAGES) //
            ? readPackagesFrom(settings.getNodeSettings(CFG_KEY_EXCLUDED_PACKAGES)) //
            : Collections.emptyList();
    }

    private static List<CondaPackageSpec> readPackagesFrom(final NodeSettingsRO subSettings)
        throws InvalidSettingsException {
        final String[] names = subSettings.getStringArray(CFG_KEY_PACKAGE_NAMES);
        final String[] versions = subSettings.getStringArray(CFG_KEY_PACKAGE_VERSIONS);
        final String[] builds = subSettings.getStringArray(CFG_KEY_PACKAGE_BUILDS);
        final String[] channels = subSettings.getStringArray(CFG_KEY_PACKAGE_CHANNELS);
        final int numPackages = names.length;
        if (!(versions.length == numPackages && builds.length == numPackages && channels.length == numPackages)) {
            throw new InvalidSettingsException(
                "The arrays containing the individual parts of the package specifications (in " + subSettings.getKey()
                    + ") must be of equal length.");
        }
        final List<CondaPackageSpec> packages = new ArrayList<>(numPackages);
        for (int i = 0; i < numPackages; i++) {
            packages.add(new CondaPackageSpec(names[i], versions[i], builds[i], channels[i]));
        }
        return packages;
    }
}
