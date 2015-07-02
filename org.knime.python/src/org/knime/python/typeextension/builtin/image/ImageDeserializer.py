# Deserializing ndarray as tiff stream

from io import BytesIO
from PIL import Image

# Check if images can be put into data frame
_works = None

# deserialize TiffFile from ByteStream
def deserialize(bytes):
	global _works
	image = Image.open(BytesIO(bytes))
	if _works is None:
		try:
			from pandas import DataFrame
			DataFrame({'image':[image]})
			_works = True
		except:
			_works = False
	if not _works:
		raise Exception('Error while adding Image to DataFrame, this does only work with pandas >= 0.8.0')
	return image
