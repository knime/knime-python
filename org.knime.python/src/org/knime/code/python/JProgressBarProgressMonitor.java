/*
 * ------------------------------------------------------------------------
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.code.python;

import javax.swing.JProgressBar;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.NodeProgressMonitor;
import org.knime.core.node.workflow.NodeProgressListener;

/**
 * Implementation of a {@link NodeProgressMonitor} holding a
 * {@link JProgressBar}.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
public class JProgressBarProgressMonitor implements NodeProgressMonitor {

    private final JProgressBar m_progressBar;
    private double m_progress;
    private boolean m_isCanceled;

    /**
     * Creates a {@link NodeProgressMonitor} for the given {@link JProgressBar}
     *
     * @param progressBar
     *            Progress bar that will display the progress
     */
    public JProgressBarProgressMonitor(final JProgressBar progressBar) {
        m_isCanceled = false;
        m_progressBar = progressBar;
        m_progressBar.setIndeterminate(false);
        m_progressBar.setMinimum(0);
        m_progressBar.setMaximum(100);
        m_progressBar.setValue(0);
        m_progressBar.setStringPainted(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkCanceled() throws CanceledExecutionException {
        if (m_isCanceled) {
            throw new CanceledExecutionException();
        }
    }

    public void setCanceled(final boolean canceled) {
        m_isCanceled = canceled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setProgress(final double progress) {
        m_progress = progress;
        m_progressBar.setValue((int) Math.round(progress * 100));
        m_progressBar.setString((int) Math.round(progress * 100) + "%");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double getProgress() {
        return m_progress;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setProgress(final double progress, final String message) {
        setProgress(progress);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMessage(final String message) {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setProgress(final String message) {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMessage() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setExecuteCanceled() {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        setProgress(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addProgressListener(final NodeProgressListener l) {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeProgressListener(final NodeProgressListener l) {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllProgressListener() {
        // do nothing
    }

}
