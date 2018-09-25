/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 28.08.2018 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.python2.generic;

import java.awt.Color;

import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rsyntaxtextarea.Token;
import org.fife.ui.rsyntaxtextarea.folding.FoldParserManager;
import org.knime.core.node.util.rsyntaxtextarea.KnimeSyntaxTextArea;
import org.knime.core.node.util.rsyntaxtextarea.guarded.GuardedDocument;
import org.knime.core.node.util.rsyntaxtextarea.guarded.GuardedSection;
import org.knime.core.node.util.rsyntaxtextarea.guarded.GuardedSectionsFoldParser;

/**
 * A TextArea for Python nodes that greys out and folds guarded sections.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("serial")
public class PythonTextArea extends KnimeSyntaxTextArea {

    /**
     * Creates a PySparkText area for guarded document highlighting
     */
    public PythonTextArea() {
        super(new GuardedDocument(SYNTAX_STYLE_PYTHON));
        boolean parserInstalled = FoldParserManager.get()
            .getFoldParser(SyntaxConstants.SYNTAX_STYLE_PYTHON) instanceof GuardedSectionsFoldParser;
        if (!parserInstalled) {
            FoldParserManager.get().addFoldParserMapping(SyntaxConstants.SYNTAX_STYLE_PYTHON,
                new GuardedSectionsFoldParser());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Color getForegroundForToken(final Token t) {
        if (isInGuardedSection(t.getOffset())) {
            return Color.gray;
        } else {
            return super.getForegroundForToken(t);
        }
    }

    /**
     * Returns true when offset is within a guarded section.
     *
     * @param offset the offset to test
     * @return true when offset is within a guarded section.
     */
    private boolean isInGuardedSection(final int offset) {
        if (getDocument() instanceof GuardedDocument) {
            GuardedDocument doc = (GuardedDocument)getDocument();

            for (String name : doc.getGuardedSections()) {
                GuardedSection gs = doc.getGuardedSection(name);
                if (gs.contains(offset)) {
                    return true;
                }
            }
        }
        return false;
    }

}
