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
 *   Apr 22, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.knime.python3.PythonPath.PythonPathBuilder;

/**
 * An object that provides {@link PythonGateway PythonGateways} for a particular combination of environment, launcher
 * and preloaded modules.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public interface PythonGatewayFactory {

    /**
     * Provides a PythonGateway that corresponds to the provided description.
     *
     * @param <E> the type of entry point
     * @param description describing the desired PythonGateway
     * @return a PythonGateway corresponding to the provided description
     * @throws IOException if the PythonGateway can't be created due to I/O related problems
     * @throws InterruptedException if the PythonGateway creation is interrupted (typically by the user)
     */
    <E extends PythonEntryPoint> PythonGateway<E> create(final PythonGatewayDescription<E> description)
        throws IOException, InterruptedException;


    /**
     * Implementing classes allow to customize a PythonEntryPoint after its process has been created.
     *
     * Implementing classes must provide meaningful implementations for equals and hashCode.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     * @param <E> the type of PythonEntryPoint customized by the customizer
     */
    public interface EntryPointCustomizer<E extends PythonEntryPoint> {

        /**
         * Customizes the entry point e.g. by calling setup methods and the like.
         *
         * @param entryPoint to customize
         */
        void customize(E entryPoint);
    }


    /**
     * Describes a PythonGateway including the command (i.e. the Python executable), the launch script, the type of
     * entry point, the folders to add to the Python Path and the extensions (or rather modules) to preload.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     * @param <E>
     */
    final class PythonGatewayDescription<E extends PythonEntryPoint> {

        private final PythonCommand m_command;

        private final Path m_launcherPath;

        private final Class<E> m_entryPointClass;

        private final PythonPath m_pythonPath;

        private final List<PythonExtension> m_pythonExtensions;

        private final List<EntryPointCustomizer<E>> m_entryPointCustomizers;

        private PythonGatewayDescription(final Builder<E> builder) {
            m_launcherPath = builder.m_launcherPath;
            m_command = builder.m_pythonCommand;
            m_entryPointClass = builder.m_entryPointClass;
            m_pythonPath = builder.m_pythonPath.build();
            m_pythonExtensions = List.copyOf(builder.m_pythonExtensions);
            m_entryPointCustomizers = List.copyOf(builder.m_entryPointCustomizers);
        }

        Path getLauncherPath() {
            return m_launcherPath;
        }

        PythonCommand getCommand() {
            return m_command;
        }

        Class<E> getEntryPointClass() {
            return m_entryPointClass;
        }

        PythonPath getPythonPath() {
            return m_pythonPath;
        }

        List<PythonExtension> getExtensions() {
            return m_pythonExtensions;
        }

        List<EntryPointCustomizer<E>> getCustomizers() {
            return m_entryPointCustomizers;
        }

        @Override
        public int hashCode() {
            return Objects.hash(//
                m_command, //
                m_launcherPath, //
                m_entryPointClass, //
                m_pythonPath, //
                m_pythonExtensions, //
                m_entryPointCustomizers//
            );
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof PythonGatewayDescription)) {
                return false;
            }
            final PythonGatewayDescription<?> other = (PythonGatewayDescription<?>)obj;
            return Objects.equals(other.m_command, m_command) //
                && Objects.equals(other.m_launcherPath, m_launcherPath)
                && Objects.equals(other.m_entryPointClass, m_entryPointClass)
                && Objects.equals(other.m_pythonPath, m_pythonPath)
                && Objects.equals(other.m_pythonExtensions, m_pythonExtensions)
                && Objects.equals(other.m_entryPointCustomizers, m_entryPointCustomizers);
        }

        /**
         * Creates a builder for a {@link PythonGatewayDescription}.
         *
         * @param <E> the type of entry point to Python
         * @param pythonCommand defining the Python executable
         * @param launcherPath specifying the script that establishes the connection to Java
         * @param entryPointClass the type of entry point
         * @return a builder for a PythonGatewayDescription
         */
        public static <E extends PythonEntryPoint> Builder<E> builder(final PythonCommand pythonCommand,
            final Path launcherPath, final Class<E> entryPointClass) {
            return new Builder<>(pythonCommand, launcherPath, entryPointClass);
        }

        /**
         * Builder for {@link PythonGatewayDescription}.
         *
         * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
         * @param <E> the type of entry point
         */
        public static final class Builder<E extends PythonEntryPoint> {

            private final Path m_launcherPath;

            private final PythonCommand m_pythonCommand;

            private final Class<E> m_entryPointClass;

            private final PythonPathBuilder m_pythonPath = new PythonPathBuilder();

            private final List<PythonExtension> m_pythonExtensions = new ArrayList<>();

            private final List<EntryPointCustomizer<E>> m_entryPointCustomizers = new ArrayList<>();

            private Builder(final PythonCommand pythonCommand, final Path launcherPath,
                final Class<E> entryPointClass) {
                m_launcherPath = launcherPath;
                m_pythonCommand = pythonCommand;
                m_entryPointClass = entryPointClass;
            }

            /**
             * Adds the provided extension to the list of extensions/modules that already need to be loaded by the
             * PythonGateway.
             *
             * @param extension to load
             * @return this builder
             */
            public Builder<E> withPreloaded(final PythonExtension extension) {
                m_pythonExtensions.add(extension);
                return this;
            }

            /**
             * Adds the provided entry point customizer to the customizers.
             * Customizers must
             *
             * @param customizer customizing the entry point for later use
             * @return this builder
             */
            public Builder<E> withCustomizer(final EntryPointCustomizer<E> customizer) {
                m_entryPointCustomizers.add(customizer);
                return this;
            }

            /**
             * Adds the provided path to the Python path.
             *
             * @param path to add to the Python path
             * @return this builder
             */
            public Builder<E> addToPythonPath(final Path path) {
                m_pythonPath.add(path);
                return this;
            }

            /**
             * Builds a PythonGatewayDescription.
             *
             * @return the PythonGatewayDescription
             */
            public PythonGatewayDescription<E> build() {
                return new PythonGatewayDescription<>(this);
            }
        }
    }
}
