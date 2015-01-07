import xml.dom.minidom as xmldom

def serialize(object_value):
	return str(object_value.toxml())
