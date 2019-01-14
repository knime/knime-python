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
 *   Jan 14, 2019 (marcel): created
 */
package org.knime.python2;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;
import java.util.Optional;

import org.knime.core.util.Version;

/**
 * Represents a Python module on Java side. Allows to specify the exact version of the module as well as a minimum
 * version, version range, or no version at all.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz
 * @author Christian Dietz, KNIME GmbH, Konstanz
 */
public final class PythonModuleSpec {

    private final String m_name;

    private final Version m_minVersion;

    private final Boolean m_minInclusive;

    private final Version m_maxVersion;

    private final Boolean m_maxInclusive;

    /**
     * Creates a specification of a module without a version or version range.
     *
     * @param name The fully qualified name of the module.
     */
    public PythonModuleSpec(final String name) {
        m_name = checkNotNull(name);
        m_minVersion = null;
        m_minInclusive = null;
        m_maxVersion = null;
        m_maxInclusive = null;
    }

    /**
     * Creates a specification of a module that includes a specific version.
     *
     * @param name The fully qualified name of the module.
     * @param version The version of the module.
     */
    public PythonModuleSpec(final String name, final Version version) {
        m_name = checkNotNull(name);
        m_minVersion = checkNotNull(version);
        m_minInclusive = true;
        m_maxVersion = version;
        m_maxInclusive = true;
    }

    /**
     * Creates a specification of a module that includes a minimum version.
     *
     * @param name The fully qualified name of the module.
     * @param minVersion The minimum version of the module.
     * @param minInclusive True if the version of the module may equal minVersion, false otherwise.
     */
    public PythonModuleSpec(final String name, final Version minVersion, final boolean minInclusive) {
        m_name = checkNotNull(name);
        m_minVersion = checkNotNull(minVersion);
        m_minInclusive = minInclusive;
        m_maxVersion = null;
        m_maxInclusive = null;
    }

    /**
     * Creates a specification of a module that includes a version range.
     * <P>
     * If {@code minVersion} equals {@code maxVersion}, {@code minInclusive} and {@code maxInclusive} must both be true.
     *
     * @param name The fully qualified name of the module.
     * @param minVersion The minimum version of the module.
     * @param minInclusive True if the version of the module may equal minVersion, false otherwise.
     * @param maxVersion The maximum version of the module.
     * @param maxInclusive True if the version of the module may equal maxVersion, false otherwise.
     */
    public PythonModuleSpec(final String name, final Version minVersion, final boolean minInclusive,
        final Version maxVersion, final boolean maxInclusive) {
        m_name = checkNotNull(name);
        m_minVersion = checkNotNull(minVersion);
        m_minInclusive = minInclusive;
        m_maxVersion = checkNotNull(maxVersion);
        m_maxInclusive = maxInclusive;
        if (minVersion.equals(maxVersion) && !(minInclusive && maxInclusive)) {
            throw new IllegalArgumentException("Bounds cannot be exclusive if minimum and maximum versions are equal.");
        }
    }

    /**
     * @return The name of the module.
     */
    public String getName() {
        return m_name;
    }

    /**
     * @return The exact version of the module, if specified. Refer to {@link #getMinVersion()} and
     *         {@link #getMaxVersion()} if a version range is specified instead.
     */
    public Optional<Version> getVersion() {
        if (Objects.equals(m_minVersion, m_maxVersion)) {
            return Optional.ofNullable(m_minVersion);
        } else {
            return Optional.empty();
        }
    }

    /**
     * @return The minimum version of the module, if specified. If the module has a specific version, the same version
     *         is returned by both this method and {@link #getMaxVersion()}.
     */
    public Optional<Version> getMinVersion() {
        return Optional.ofNullable(m_minVersion);
    }

    /**
     * @return Whether the {@link #getMinVersion() minimum version bound} is inclusive or not, if specified. If the
     *         module has a specific version, an optional of {@code true} is returned by both this method and
     *         {@link #getMaxInclusive()}.
     */
    public Optional<Boolean> getMinInclusive() {
        return Optional.ofNullable(m_minInclusive);
    }

    /**
     * @return The maximum version of the module, if specified. If the module has a specific version, the same version
     *         is returned by both this method and {@link #getMinVersion()}.
     */
    public Optional<Version> getMaxVersion() {
        return Optional.ofNullable(m_maxVersion);
    }

    /**
     * @return Whether the {@link #getMaxVersion() maximum version bound} is inclusive or not, if specified. If the
     *         module has a specific version, an optional of {@code true} is returned by both this method and
     *         {@link #getMinInclusive()}.
     */
    public Optional<Boolean> getMaxInclusive() {
        return Optional.ofNullable(m_maxInclusive);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_maxInclusive == null) ? 0 : m_maxInclusive.hashCode());
        result = prime * result + ((m_maxVersion == null) ? 0 : m_maxVersion.hashCode());
        result = prime * result + ((m_minInclusive == null) ? 0 : m_minInclusive.hashCode());
        result = prime * result + ((m_minVersion == null) ? 0 : m_minVersion.hashCode());
        result = prime * result + ((m_name == null) ? 0 : m_name.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final PythonModuleSpec other = (PythonModuleSpec)obj;
        return Objects.equals(m_name, other.m_name) //
            && Objects.equals(m_minVersion, other.m_minVersion) //
            && Objects.equals(m_minInclusive, other.m_minInclusive) //
            && Objects.equals(m_maxVersion, other.m_maxVersion) //
            && Objects.equals(m_maxInclusive, other.m_maxInclusive);
    }

    @Override
    public String toString() {
        // Format expected on Python side (PythonKernelTester.py).
        final String inclusive = "inclusive";
        final String exclusive = "exclusive";
        String stringRepresentation = m_name;
        if (m_minVersion != null) {
            stringRepresentation += "=" + m_minVersion + ":" + (m_minInclusive ? inclusive : exclusive);
            if (m_maxVersion != null) {
                stringRepresentation += ":" + m_maxVersion + ":" + (m_maxInclusive ? inclusive : exclusive);
            }
        }
        return stringRepresentation;
    }
}
