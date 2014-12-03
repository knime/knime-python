package org.knime.python.typeextension.example;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;

import org.knime.core.data.DataCell;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.data.xml.XMLCell;
import org.knime.core.data.xml.XMLCellFactory;
import org.knime.python.typeextension.Deserializer;
import org.knime.python.typeextension.DeserializerFactory;
import org.xml.sax.SAXException;

public class XMLDeserializerFactory extends DeserializerFactory {
	
	public XMLDeserializerFactory() {
		super(XMLCell.TYPE);
	}
	
	@Override
	public Deserializer createDeserializer() {
		return new XMLDeserializer();
	}

	private class XMLDeserializer implements Deserializer {
		
		@Override
		public DataCell deserialize(byte[] bytes, final FileStoreFactory fileStoreFactory) throws IOException {
			try {
				return XMLCellFactory.create(new String(bytes));
			} catch (ParserConfigurationException | SAXException
					| XMLStreamException e) {
				throw new IOException(e.getMessage(), e);
			}
		}
		
	}

}
