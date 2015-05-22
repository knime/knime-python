import xml.dom.minidom as xmldom

def deserialize(bytes):
	return xmldom.parseString(bytes)
