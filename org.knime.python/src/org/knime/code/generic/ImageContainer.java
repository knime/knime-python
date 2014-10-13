package org.knime.code.generic;

import java.awt.image.BufferedImage;

import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.ImageTranscoder;
import org.w3c.dom.svg.SVGDocument;

public class ImageContainer {
	
	private BufferedImage m_bufferedImage;
	
	private SVGDocument m_svgDocument;
	
	public ImageContainer(final BufferedImage bufferedImage) {
		m_bufferedImage = bufferedImage;
		m_svgDocument = null;
	}
	
	public ImageContainer(final SVGDocument svgDocument) throws TranscoderException {
		m_svgDocument = svgDocument;
        BufferedImageTranscoder t = new BufferedImageTranscoder();
        t.transcode(new TranscoderInput(svgDocument), null);
        m_bufferedImage = t.getBufferedImage();
	}
	
	public boolean hasSvgDocument() {
		return m_svgDocument != null;
	}

	public BufferedImage getBufferedImage() {
		return m_bufferedImage;
	}
	
	public SVGDocument getSvgDocument() {
		return m_svgDocument;
	}

	private static class BufferedImageTranscoder extends ImageTranscoder {
		protected BufferedImage bufferedImage;

		public BufferedImage createImage(int width, int height) {
			return new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
		}

		public void writeImage(BufferedImage img, TranscoderOutput output) throws TranscoderException {
			bufferedImage = img;
		}

		public BufferedImage getBufferedImage() {
			return bufferedImage;
		}
	}
	
}
