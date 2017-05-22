# Serializing TiffFile image on byte stream

from io import BytesIO
from PIL import Image

def serialize(pil_image):
	buffer = BytesIO()
	pil_image.save(buffer, 'png')
	return buffer.getvalue()

