# Deserializing ndarray as tiff stream

from io import BytesIO
from PIL import Image

# deserialize TiffFile from ByteStream
def deserialize(bytes):
	return Image.open(BytesIO(bytes))
