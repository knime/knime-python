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
 *   Apr 20, 2021 (benjamin): created
 */
package org.knime.python3.arrow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.schema.traits.DefaultListDataTraits;
import org.knime.core.table.schema.traits.DefaultStructDataTraits;

// TODO(extensiontypes) This should be replaced with something that works on virtual types/extension types
@SuppressWarnings("javadoc")
public class PythonColumnarSchemaBuilder {

    private final List<DataSpec> m_specs;

    private final List<DataTraits> m_traits;

    private boolean m_built;

    public PythonColumnarSchemaBuilder() {
        m_specs = new ArrayList<>();
        m_traits = new ArrayList<>();
        m_built = false;
    }

    private static DataTraits emptyTraitsForSpec(final DataSpec spec) {
        if (spec instanceof ListDataSpec) {
            return new DefaultListDataTraits(emptyTraitsForSpec(((ListDataSpec)spec).getInner()));
        } else if (spec instanceof StructDataSpec) {
            return new DefaultStructDataTraits(
                Arrays.stream(((StructDataSpec)spec)
                    .getInner()).map(PythonColumnarSchemaBuilder::emptyTraitsForSpec)
                    .toArray(DataTraits[]::new));
        }

        return DefaultDataTraits.EMPTY;
    }

    public void addColumn(final DataSpec spec, final DataTraits traits) {
        if (m_built) {
            throw new IllegalStateException("Cannot add columns after the Schema has been build.");
        }
        m_specs.add(spec);
        m_traits.add(traits);
    }

    public void addColumn(final DataSpec spec) {
        addColumn(spec, emptyTraitsForSpec(spec));
    }

    public ColumnarSchema build() {
        m_built = true;
        return new DefaultColumnarSchema(m_specs.toArray(DataSpec[]::new), m_traits.toArray(DataTraits[]::new));
    }
}
