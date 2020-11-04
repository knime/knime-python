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
package org.knime.python2;

import java.util.Objects;

/**
 * Describes a package inside a Conda environment.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class CondaPackageSpec {

    private final String m_name;

    private final String m_version;

    private final String m_build;

    private final String m_channel;

    /**
     * Creates a new specification of a Conda package.
     *
     * @param name The name of the package.
     * @param version The version of the package.
     * @param build The build spec of the package.
     * @param channel The source channel from which the package was retrieved.
     */
    public CondaPackageSpec(final String name, final String version, final String build, final String channel) {
        m_name = name;
        m_version = version;
        m_build = build;
        m_channel = channel;
    }

    /**
     * @return The name of the package.
     */
    public String getName() {
        return m_name;
    }

    /**
     * @return The version of the package.
     */
    public String getVersion() {
        return m_version;
    }

    /**
     * @return The build string of the package.
     */
    public String getBuild() {
        return m_build;
    }

    /**
     * @return The channel from which the package was retrieved.
     */
    public String getChannel() {
        return m_channel;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_name, m_version, m_build, m_channel);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof CondaPackageSpec)) {
            return false;
        }
        final CondaPackageSpec other = (CondaPackageSpec)obj;
        return Objects.equals(other.m_name, m_name) //
            && Objects.equals(other.m_version, m_version) //
            && Objects.equals(other.m_build, m_build) //
            && Objects.equals(other.m_channel, m_channel);
    }

    @Override
    public String toString() {
        return m_name + "=" + m_version + "=" + m_build + " (" + m_channel + ")";
    }
}
