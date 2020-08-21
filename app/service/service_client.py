import logging

import pyfastocloud.socket.gevent as gsocket
from bson.objectid import ObjectId
from pyfastocloud.fastocloud_client import Commands
from pyfastocloud.json_rpc import Request, Response
from pyfastocloud_node.service.service_client import ServiceClient
from pyfastocloud_node.service.stream_handler import IStreamHandler


class ServiceClientPanel(ServiceClient):
    HTTP_HOST = 'http_host'
    VODS_HOST = 'vods_host'
    CODS_HOST = 'cods_host'

    def __init__(self, sid: ObjectId, host: str, port: int, handler: IStreamHandler):
        super().__init__(host, port, handler, gsocket)
        self.id = sid
        self.__set_runtime_fields_next()
        logging.debug('Created Server: {0}:{1}'.format(host, port))

    @property
    def http_host(self) -> str:
        return self._http_host

    @property
    def vods_host(self) -> str:
        return self._vods_host

    @property
    def cods_host(self) -> str:
        return self._cods_host

    # handler
    def process_response(self, client, req: Request, resp: Response):
        if not req:
            return

        if req.method == Commands.ACTIVATE_COMMAND and resp.is_message():
            if self._handler:
                result = resp.result
                self.__set_runtime_fields_next(result[ServiceClientPanel.HTTP_HOST],
                                               result[ServiceClientPanel.VODS_HOST],
                                               result[ServiceClientPanel.CODS_HOST])

        super().process_response(client, req, resp)

    # private
    def __set_runtime_fields_next(self, http_host=None, vods_host=None, cods_host=None):
        self._http_host = http_host
        self._vods_host = vods_host
        self._cods_host = cods_host

