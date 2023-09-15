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
 *   Sep 21, 2022 (benjamin): created
 */
package org.knime.python3.scripting.nodes2;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;

import javax.imageio.ImageIO;

import org.apache.batik.anim.dom.SAXSVGDocumentFactory;
import org.apache.batik.util.XMLResourceDescriptor;
import org.knime.base.data.xml.SvgCell;
import org.knime.base.data.xml.SvgImageContent;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.image.png.PNGImageContent;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.image.ImagePortObject;
import org.knime.core.node.port.image.ImagePortObjectSpec;
import org.knime.core.node.port.inactive.InactiveBranchPortObject;
import org.knime.core.util.PathUtils;
import org.knime.python2.port.PickledObjectFile;
import org.knime.python2.port.PickledObjectFileStorePortObject;
import org.knime.python3.PythonDataSource;
import org.knime.python3.arrow.PythonArrowDataSink;
import org.knime.python3.arrow.PythonArrowDataSource;
import org.knime.python3.arrow.PythonArrowTableConverter;
import org.knime.python3.views.PythonNodeViewSink;

/**
 * Static utilities for getting KNIME port data to a Python process and back.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class PythonIOUtils {

    private PythonIOUtils() {
        // Utility class
    }

    /**
     * Create an array of Python data sources for the given input ports. The input ports can be either a
     * {@link BufferedDataTable} or a {@link PickledObjectFileStorePortObject}.
     *
     * @param data a list of port objects. Only {@link BufferedDataTable} and {@link PickledObjectFileStorePortObject}
     *            are supported.
     * @param tableConverter a table converter that is used to convert the {@link BufferedDataTable}s to Python sources
     * @param exec for progress reporting and cancellation
     * @return an array of Python data sources
     * @throws IOException if the path to a pickled file could not be created or a {@link BufferedDataTable} couldn't be
     *             written
     * @throws CanceledExecutionException if the execution was canceled
     * @throws IllegalArgumentException if the data contains unsupported port types
     */
    static PythonDataSource[] createSources(final PortObject[] data, final PythonArrowTableConverter tableConverter,
        final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
        final var pickledPortObjects = Arrays.stream(data) //
            .filter(PickledObjectFileStorePortObject.class::isInstance) //
            .toArray(PickledObjectFileStorePortObject[]::new);
        final var tablePortObjects = Arrays.stream(data) //
            .filter(BufferedDataTable.class::isInstance) //
            .toArray(BufferedDataTable[]::new);

        // Make sure that all ports are tables or pickled port objects
        if (pickledPortObjects.length + tablePortObjects.length < data.length) {
            throw new IllegalArgumentException("Unsupported port type connected. This is an implementation error.");
        }

        // Progress handling
        final var pickledProgressWeight = 1;
        final var tableProgressWeight = 3;
        final var totalProgress =
            pickledPortObjects.length * pickledProgressWeight + tablePortObjects.length * tableProgressWeight;
        var progress = 0;

        // Pickled object to sources
        final var pickledSources = new ArrayList<PickledObjectDataSource>();
        for (int i = 0; i < pickledPortObjects.length; i++) {
            exec.setMessage("Setting up pickled object " + i);
            pickledSources.add(PickledObjectDataSource.fromPortObject(pickledPortObjects[i]));
            exec.checkCanceled();
            progress += pickledProgressWeight;
            exec.setProgress(progress / (double)totalProgress);
        }

        // Tables to sources
        exec.setMessage("Setting up input tables");
        final PythonArrowDataSource[] tableSources = tableConverter.createSources(tablePortObjects, exec);
        exec.checkCanceled();
        exec.setProgress(1.0);

        return Stream.concat(pickledSources.stream(), Arrays.stream(tableSources)).toArray(PythonDataSource[]::new);
    }

    /**
     * Get all output tables from the Python process.
     *
     * @param numOutTables the number of tables to retrieve
     * @param pythonEntryPoint the entry point to communicate with the Python process
     * @param tableConverter a table converter that is used to convert the sinks to {@link BufferedDataTable}l
     * @param exec for table creation as well as progress reporting and cancellation
     * @return the {@link BufferedDataTable}s
     * @throws IOException if any of the tables contains duplicate row keys
     * @throws CanceledExecutionException if the execution is cancelled by the user
     */
    static BufferedDataTable[] getOutputTables(final int numOutTables, final PythonScriptingEntryPoint pythonEntryPoint,
        final PythonArrowTableConverter tableConverter, final ExecutionContext exec)
        throws IOException, CanceledExecutionException {
        final var sinks = new ArrayList<PythonArrowDataSink>();
        exec.setMessage("Retrieving output tables");
        for (int i = 0; i < numOutTables; i++) {
            sinks.add(pythonEntryPoint.getOutputTable(i));
            exec.checkCanceled();
        }
        final var tables = tableConverter.convertToTables(sinks, exec);
        exec.setProgress(1.0);
        return tables;
    }

    /**
     * Get all output images from the Python process.
     *
     * @param numOutImages the number of images to retrieve
     * @param pythonEntryPoint the entry point to communicate with the Python process
     * @param exec for progress reporting and cancellation
     * @return the {@link ImagePortObject}s (or {@link InactiveBranchPortObject} if an image is invalid)
     * @throws IOException if writing the image to a file or reading it back failed
     * @throws CanceledExecutionException if the execution is cancelled by the user
     */
    static PortObject[] getOutputImages(final int numOutImages, final PythonScriptingEntryPoint pythonEntryPoint,
        final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
        if (numOutImages == 0) {
            // Return immediately if there are no output images (don't create a temporary directory)
            return new PortObject[0];
        }

        final var images = new ArrayList<PortObject>();
        Path tmpPath = null;
        try {
            tmpPath = PathUtils.createTempDir("python_images");
            for (int i = 0; i < numOutImages; i++) {
                exec.setMessage("Retrieving image " + i);
                // Write the image to a temporary file
                final Path imgPath = tmpPath.resolve("" + i);
                pythonEntryPoint.writeOutputImage(i, imgPath.toAbsolutePath().toString());
                images.add(readImage(imgPath));
                exec.checkCanceled();
                exec.setProgress((i + 1) / (double)numOutImages);
            }
        } finally {
            if (tmpPath != null) {
                PathUtils.deleteDirectoryIfExists(tmpPath);
            }
        }

        return images.toArray(PortObject[]::new);
    }

    /**
     * Get all output objects from the Python process.
     *
     * @param numOutObjects the number of objects to retrieve
     * @param pythonEntryPoint the entry point to communicate with the Python process
     * @param exec for file store creation as well as progress reporting and cancellation
     * @return the {@link PickledObjectFileStorePortObject}s
     * @throws IOException if the file store creation or writing the file failed
     * @throws CanceledExecutionException if the execution is cancelled by the user
     */
    static PickledObjectFileStorePortObject[] getOutputObjects(final int numOutObjects,
        final PythonScriptingEntryPoint pythonEntryPoint, final ExecutionContext exec)
        throws IOException, CanceledExecutionException {
        var fileStoreFactory = FileStoreFactory.createFileStoreFactory(exec);
        var objects = new ArrayList<PickledObjectFileStorePortObject>();
        for (int i = 0; i < numOutObjects; i++) {
            exec.setMessage("Retrieving pickled object " + i);
            final var idx = i;
            objects.add( //
                PickledObjectFileStorePortObject.create( //
                    fileStoreFactory, //
                    file -> {
                        pythonEntryPoint.writeOutputObject(idx, file.getAbsolutePath());
                        var type = pythonEntryPoint.getOutputObjectType(idx);
                        var stringRep = pythonEntryPoint.getOutputObjectStringRepr(idx);
                        return new PickledObjectFile(file, type, stringRep);
                    } //
                ) //
            );
            exec.checkCanceled();
            exec.setProgress((i + 1) / (double)numOutObjects);
        }
        return objects.toArray(PickledObjectFileStorePortObject[]::new);
    }

    /**
     * Write the output view to a new temporary file and return the path to the file. The caller must delete the file
     * when it is not needed anymore
     *
     * @throws IOException if the temporary file could not be created
     */
    static Path getOutputView(final PythonScriptingEntryPoint pythonEntryPoint) throws IOException {
        final var path = PathUtils.createTempFile("output_view", ".html");
        pythonEntryPoint.getOutputView(new PythonNodeViewSink(path.toAbsolutePath().toString()));
        return path;
    }

    /**
     * Read an {@link ImagePortObject} from the given path. Returns an {@link InactiveBranchPortObject} if the file does
     * not contain an image.
     */
    private static PortObject readImage(final Path path) throws IOException {
        // Try to read as an SVG
        try (var reader = new BufferedReader(new InputStreamReader(Files.newInputStream(path)))) {
            final var factory = new SAXSVGDocumentFactory(XMLResourceDescriptor.getXMLParserClassName());
            final var svgDoc = factory.createSVGDocument("file:/file.svg", reader);
            return new ImagePortObject(new SvgImageContent(svgDoc, true), new ImagePortObjectSpec(SvgCell.TYPE));
        } catch (final IOException | IllegalArgumentException e) {
            // IOException:
            // - Input stream could not be read
            // - SVG document could not be parsed

            // IllegalArgumentException:
            // - The check in the SvgImageContent constructor failed

            // Ignore all issues and try to read it as an PNG
            NodeLogger.getLogger(PythonIOUtils.class) //
                .debug("Reading the image as SVG failed. Trying to read as PNG.", e);
        }

        // Read as an PNG
        final BufferedImage image;
        try (var input = new BufferedInputStream(Files.newInputStream(path))) {
            image = ImageIO.read(input);
        }
        // NB: image might be null if the input is no valid image
        if (image != null) {
            return new ImagePortObject(new PNGImageContent(imageToPngBytes(image)),
                new ImagePortObjectSpec(PNGImageContent.TYPE));
        }

        // This follows the behavior of the old Python Labs node
        return InactiveBranchPortObject.INSTANCE;
    }

    /** Get the bytes of the PNG representation of the given image object */
    private static byte[] imageToPngBytes(final BufferedImage image) throws IOException {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            ImageIO.write(image, "png", os);
            return os.toByteArray();
        }
    }
}
