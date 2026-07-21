# bound.token.ext.vision
## @lineage: bound.surface.token.ext.vision
## @lineage: bound.adapter.surface.token.ext.vision
## @lineage: anchor.provider.token.ext.vision
## @lineage: anchor.registry.provider.token.ext.vision
import base64
import io
import struct
from typing import Tuple, Union

from bound.token.url_utils import SafeHttpClient
from bound.transport.bridge.client import get_client
from bound.registry.model.config.constants import (
    DEFAULT_IMAGE_HEIGHT,
    DEFAULT_IMAGE_WIDTH,
    MAX_IMAGE_URL_DOWNLOAD_SIZE_MB,
    MAX_LONG_SIDE_FOR_IMAGE_HIGH_RES,
    MAX_SHORT_SIDE_FOR_IMAGE_HIGH_RES,
    MAX_TILE_HEIGHT,
    MAX_TILE_WIDTH,
)
from watcher.plane.emitter import get_emitter

log = get_emitter("ext.vision")

class VisionMetadataExtractor:
    """
    @manifold: I/O Bound Vision Extractor
    @desc: 이미지 URL이나 Base64 데이터를 파싱하여 가로/세로 길이를 알아냅니다.
           동기적 네트워크 통신이 발생할 수 있으므로 핫 패스(Hot Path)에서는 호출을 지양해야 합니다.
    """
    @staticmethod
    def get_image_dimensions(data: str) -> Tuple[int, int]:
        img_data = None
        if data.startswith(("http://", "https://")):
            try:
                client = get_client(is_async=False)
                safe_client = SafeHttpClient(client)
                response = safe_client.get(data)
                max_bytes = int(MAX_IMAGE_URL_DOWNLOAD_SIZE_MB * 1024 * 1024)
                
                content_length = response.headers.get("Content-Length")
                if content_length is None or int(content_length) <= max_bytes:
                    body = response.read()
                    if len(body) <= max_bytes:
                        img_data = body
            except Exception as e:
                log.warning(f"[Vision] URL에서 이미지 차원 추출 실패: {e}")

        if img_data is None:
            try:
                _header, encoded = data.split(",", 1) if "," in data else ("", data)
                img_data = base64.b64decode(encoded)
            except Exception as e:
                log.warning(f"[Vision] Base64 이미지 데이터 디코딩 실패: {e}")
                return DEFAULT_IMAGE_WIDTH, DEFAULT_IMAGE_HEIGHT

        img_type = VisionMetadataExtractor._get_image_type(img_data)

        try:
            if img_type == "png":
                w, h = struct.unpack(">LL", img_data[16:24])
                return w, h
            elif img_type == "gif":
                w, h = struct.unpack("<HH", img_data[6:10])
                return w, h
            elif img_type == "jpeg":
                with io.BytesIO(img_data) as fhandle:
                    fhandle.seek(0)
                    size = 2
                    ftype = 0
                    while not 0xC0 <= ftype <= 0xCF or ftype in (0xC4, 0xC8, 0xCC):
                        fhandle.seek(size, 1)
                        byte = fhandle.read(1)
                        while ord(byte) == 0xFF:
                            byte = fhandle.read(1)
                        ftype = ord(byte)
                        size = struct.unpack(">H", fhandle.read(2))[0] - 2
                    fhandle.seek(1, 1)
                    h, w = struct.unpack(">HH", fhandle.read(4))
                return w, h
            elif img_type == "webp":
                if img_data[12:16] == b"VP8X":
                    w = struct.unpack("<I", img_data[24:27] + b"\x00")[0] + 1
                    h = struct.unpack("<I", img_data[27:30] + b"\x00")[0] + 1
                    return w, h
        except struct.error:
            pass

        return DEFAULT_IMAGE_WIDTH, DEFAULT_IMAGE_HEIGHT

    @staticmethod
    def _get_image_type(image_data: bytes) -> Union[str, None]:
        if image_data[0:8] == b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a":
            return "png"
        if image_data[0:4] == b"GIF8" and image_data[5:6] == b"a":
            return "gif"
        if image_data[0:3] == b"\xff\xd8\xff":
            return "jpeg"
        if image_data[4:8] == b"ftyp":
            return "heic"
        if image_data[0:4] == b"RIFF" and image_data[8:12] == b"WEBP":
            return "webp"
        return None

    @staticmethod
    def calculate_tiles_needed(width: int, height: int) -> int:
        """해상도에 따른 OpenAI 호환 타일 개수를 계산합니다."""
        max_short_side = MAX_SHORT_SIDE_FOR_IMAGE_HIGH_RES
        max_long_side = MAX_LONG_SIDE_FOR_IMAGE_HIGH_RES

        if width <= max_short_side and height <= max_short_side:
            resized_width, resized_height = width, height
        else:
            aspect_ratio = max(width, height) / min(width, height)
            if width <= height:
                resized_width = max_short_side
                resized_height = int(resized_width * aspect_ratio)
                if resized_height > max_long_side:
                    resized_height = max_long_side
                    resized_width = int(resized_height / aspect_ratio)
            else:
                resized_height = max_short_side
                resized_width = int(resized_height * aspect_ratio)
                if resized_width > max_long_side:
                    resized_width = max_long_side
                    resized_height = int(resized_width / aspect_ratio)

        tiles_across = (resized_width + MAX_TILE_WIDTH - 1) // MAX_TILE_WIDTH
        tiles_down = (resized_height + MAX_TILE_HEIGHT - 1) // MAX_TILE_HEIGHT
        return tiles_across * tiles_down