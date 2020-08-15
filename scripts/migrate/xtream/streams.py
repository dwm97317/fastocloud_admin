import json

import pyfastocloud_models.constants as constants
from pyfastocloud_models.stream.entry import ProxyStream
from pyfastocloud_models.utils.utils import is_valid_http_url

from app.service.service import ServiceSettings


def import_streams_to_server(db, server: ServiceSettings):
    cursor = db.cursor(dictionary=True)
    sql = 'SELECT stream_source, stream_display_name, stream_icon, channel_id from streams'
    cursor.execute(sql)
    sql_streams = cursor.fetchall()

    for sql_entry in sql_streams:
        stream = ProxyStream.make_stream(server)
        urls = json.loads(sql_entry['stream_source'])
        if not len(urls):
            continue

        stream.output[0].uri = urls[0]
        stream.name = sql_entry['stream_display_name']
        tvg_logo = sql_entry['stream_icon']
        if len(tvg_logo) < constants.MAX_URI_LENGTH:
            if is_valid_http_url(tvg_logo, timeout=0.1):
                stream.tvg_logo = tvg_logo
        epg_id = sql_entry['channel_id']
        if epg_id:
            stream.tvg_id = epg_id

        stream.save(server)
        server.streams.append(stream)

    server.save()
    cursor.close()
