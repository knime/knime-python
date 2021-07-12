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
 *   Apr 12, 2021 (benjamin): created
 */
package org.knime.python3.arrow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.knime.python3.PythonCommand;
import org.knime.python3.PythonDataCallback;
import org.knime.python3.PythonDataProvider;
import org.knime.python3.PythonEntryPoint;
import org.knime.python3.PythonExtension;
import org.knime.python3.PythonGateway;
import org.knime.python3.PythonModuleKnime;
import org.knime.python3.PythonPath;
import org.knime.python3.PythonPath.PythonPathBuilder;
import org.knime.python3.SimplePythonCommand;

@SuppressWarnings("javadoc")
public class TestUtils {

    private TestUtils() {
        // Static utility class
    }

    /**
     * Create a temporary file which is deleted on exit.
     *
     * @return the file
     * @throws IOException if the file could not be created
     */
    public static Path createTmpKNIMEArrowPath() throws IOException {
        final Path path = Files.createTempFile("KNIME-" + UUID.randomUUID().toString(), ".knarrow");
        path.toFile().deleteOnExit();
        return path;
    }

    public static PythonGateway<ArrowTestsEntryPoint> openPythonGateway() throws IOException {
        // TODO get the command from an environment variable or similar
        final PythonCommand command =
            new SimplePythonCommand("/home/benjamin/apps/miniconda3/envs/sharedmem/bin/python");
        final String launcherPath = Paths.get(System.getProperty("user.dir"), "py", "tests_launcher.py").toString();
        final PythonPath pythonPath = (new PythonPathBuilder()) //
            .add(PythonModuleKnime.getPythonModule()) //
            .add(PythonModuleKnimeArrow.getPythonModule()) //
            .build();
        final List<PythonExtension> extensions = Collections.singletonList(PythonArrowExtension.INSTANCE);

        return new PythonGateway<>(command, launcherPath, ArrowTestsEntryPoint.class, extensions, pythonPath);
    }

    // TODO(benjamin) remove unused
    public interface ArrowTestsEntryPoint extends PythonEntryPoint {

        void testSimpleComputation(PythonArrowDataProvider dataProvider, PythonArrowDataCallback dataCallback);

        void testLocalDate(PythonArrowDataProvider dataProvider);

        void testZonedDateTime(PythonArrowDataProvider dataProvider);

        void testStruct(PythonArrowDataProvider dataProvider);

        void testMultipleInputsOutputs(List<PythonDataProvider> dataProviders, List<PythonDataCallback> dataCallbacks);

        void testTypeToPython(String type, PythonDataProvider dataProvider);
    }
}
