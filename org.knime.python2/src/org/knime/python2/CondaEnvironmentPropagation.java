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
 *   Nov 18, 2020 (marcel): created
 */
package org.knime.python2;

import java.util.Collections;
import java.util.Set;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.config.Config;
import org.knime.core.node.config.base.ConfigEntries;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.node.workflow.VariableTypeExtension;
import org.knime.python2.Conda.CondaEnvironmentSpec;

import com.google.common.collect.Sets;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class CondaEnvironmentPropagation {

    /**
     * The name of the flow variable output by the <i>Conda Environment Propagation</i> node that propagates the
     * validated/recreated Conda environment. We define the variable here to avoid other Python-based extensions (such
     * as deep learning or MDF) having dependencies directly on the node.
     */
    public static final String FLOW_VAR_NAME = "conda.environment";

    private CondaEnvironmentPropagation() {}

    public static final class CondaEnvironment {

        private CondaEnvironmentSpec m_identifier;

        public CondaEnvironment(final CondaEnvironmentSpec identifier) {
            m_identifier = identifier;
        }

        public CondaEnvironmentSpec getIdentifier() {
            return m_identifier;
        }

        // TODO: clean up to-string vs. cfg-key names vs. valueToString below
        @Override
        public String toString() {
            return String.format("{name: %s, prefix: %s}", m_identifier.getName(), m_identifier.getDirectoryPath());
        }
    }

    public static final class CondaEnvironmentType extends VariableType<CondaEnvironment> {

        /**
         * The singleton instance of the {@link CondaEnvironmentType} type.
         */
        public static final CondaEnvironmentType INSTANCE = new CondaEnvironmentType();

        /**
         * Create sub-settings, inner entries may otherwise interfere with entries created by the framework (e.g. "name"
         * holds the actual name of the flow variable).
         */
        private static final String CFG_KEY_VALUE = "value";

        private static final String CFG_KEY_ENV_NAME = "name";

        private static final String CFG_KEY_ENV_DIRECTORY_PATH = "directory";

        private static final Set<VariableType<?>> CONVERTIBLE =
            Collections.unmodifiableSet(Sets.newHashSet(StringType.INSTANCE));

        private CondaEnvironmentType() {}

        @Override
        public Class<CondaEnvironment> getSimpleType() {
            return CondaEnvironment.class;
        }

        @Override
        protected VariableValue<CondaEnvironment> loadValue(final NodeSettingsRO settings)
            throws InvalidSettingsException {
            final NodeSettingsRO subSettings = settings.getNodeSettings(CFG_KEY_VALUE);
            final String name = subSettings.getString(CFG_KEY_ENV_NAME);
            final String directoryPath = subSettings.getString(CFG_KEY_ENV_DIRECTORY_PATH);
            return new CondaEnvironmentValue(new CondaEnvironment(new CondaEnvironmentSpec(name, directoryPath)));
        }

        @Override
        protected void saveValue(final NodeSettingsWO settings, final VariableValue<CondaEnvironment> v) {
            final CondaEnvironment environment = ((CondaEnvironmentValue)v).get();
            final NodeSettingsWO subSettings = settings.addNodeSettings(CFG_KEY_VALUE);
            final CondaEnvironmentSpec identifier = environment.getIdentifier();
            subSettings.addString(CFG_KEY_ENV_NAME, identifier.getName());
            subSettings.addString(CFG_KEY_ENV_DIRECTORY_PATH, identifier.getDirectoryPath());
        }

        @Override
        protected VariableValue<CondaEnvironment> newValue(final CondaEnvironment v) {
            return new CondaEnvironmentValue(v);
        }

        @Override
        protected VariableValue<CondaEnvironment> defaultValue() {
            return new CondaEnvironmentValue(new CondaEnvironment(new CondaEnvironmentSpec("", "")));
        }

        @Override
        protected boolean canOverwrite(final Config config, final String configKey) {
            return config.getEntry(configKey).getType() == ConfigEntries.xstring;
        }

        @Override
        protected void overwrite(final CondaEnvironment value, final Config config, final String configKey)
            throws InvalidConfigEntryException {
            config.addString(configKey, valueToString(value));
        }

        @Override
        protected boolean canCreateFrom(final Config config, final String configKey) {
            // TODO: this will only be possible once we use more than just the directory path for overwriting (see
            // below)
            return false;
        }

        @Override
        protected CondaEnvironment createFrom(final Config config, final String configKey)
            throws InvalidSettingsException, InvalidConfigEntryException {
            // TODO: this will only be possible once we use more than just the directory path for overwriting (see
            // below)
            return null;
        }

        @Override
        public Set<VariableType<?>> getConvertibleTypes() {
            return CONVERTIBLE;
        }

        @Override
        protected <U> U getAs(final CondaEnvironment value, final VariableType<U> conversionTarget) {
            if (StringType.INSTANCE.equals(conversionTarget)) {
                @SuppressWarnings("unchecked") // Type safety has been ensured.
                final U casted = (U)valueToString(value);
                return casted;
            } else {
                throw createNotConvertibleException(this, conversionTarget);
            }
        }

        private static String valueToString(final CondaEnvironment value) {
            // TODO: we may want to include more than just the environment directory path here
            return value.getIdentifier().getDirectoryPath();
        }

        public static final class CondaEnvironmentTypeExtension implements VariableTypeExtension {

            @Override
            public VariableType<?> getVariableType() {
                return INSTANCE;
            }
        }

        private static final class CondaEnvironmentValue extends VariableValue<CondaEnvironment> {

            public CondaEnvironmentValue(final CondaEnvironment value) {
                super(INSTANCE, value);
            }

            @Override
            protected CondaEnvironment get() { // NOSONAR -- We must override this method for visibility.
                return super.get();
            }
        }
    }
}
