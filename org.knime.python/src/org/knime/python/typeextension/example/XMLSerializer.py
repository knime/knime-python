import xml.dom.minidom as xmldom

def serialize(object_value):
	return str(object_value.toprettyxml())

def deserialize(bytes):
	return xmldom.parseString(bytes)

