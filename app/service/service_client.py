import pyfastocloud.socket.gevent as gsocket
import pyfastocloud_models.constants as constants
from bson.objectid import ObjectId
from pyfastocloud.client_constants import ClientStatus
from pyfastocloud.client_handler import IClientHandler
from pyfastocloud.fastocloud_client import FastoCloudClient, Commands, RequestReturn
from pyfastocloud.json_rpc import Request, Response

from app.service.stream_handler import IStreamHandler


class OperationSystem(object):
    __slots__ = ['name', 'version', 'arch']

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if key in self.__slots__:
                setattr(self, key, value)

    def __str__(self):
        return '{0} {1}({2})'.format(self.name, self.version, self.arch)


class ServiceClient(IClientHandler):
    HTTP_HOST = 'http_host'
    VODS_HOST = 'vods_host'
    CODS_HOST = 'cods_host'
    PROJECT = 'project'
    VERSION = 'version'
    EXP_TIME = 'expiration_time'
    OS = 'os'

    @staticmethod
    def get_log_service_path(host: str, port: int, sid: str):
        return constants.DEFAULT_SERVICE_LOG_PATH_TEMPLATE_3SIS.format(host, port, sid)

    @staticmethod
    def get_log_stream_path(host: str, port: int, stream_id: str):
        return constants.DEFAULT_STREAM_LOG_PATH_TEMPLATE_3SIS.format(host, port, stream_id)

    @staticmethod
    def get_pipeline_stream_path(host: str, port: int, stream_id: str):
        return constants.DEFAULT_STREAM_PIPELINE_PATH_TEMPLATE_3SIS.format(host, port, stream_id)

    def __init__(self, sid: ObjectId, host: str, port: int, handler: IStreamHandler):
        self.id = sid
        self._request_id = 0
        self._handler = handler
        self._client = FastoCloudClient(host, port, self, gsocket)
        self._set_runtime_fields()

    def connect(self):
        self._client.connect()

    def is_connected(self):
        return self._client.is_connected()

    def socket(self):
        return self._client.socket()

    def recv_data(self) -> bool:
        data = self._client.read_command()
        if not data:
            return False

        self._client.process_commands(data)
        return True

    def status(self) -> ClientStatus:
        return self._client.status()

    def disconnect(self):
        self._client.disconnect()

    def activate(self, license_key: str) -> RequestReturn:
        return self._client.activate(self._gen_request_id(), license_key)

    def ping_service(self) -> RequestReturn:
        return self._client.ping(self._gen_request_id())

    def stop_service(self, delay: int) -> RequestReturn:
        return self._client.stop_service(self._gen_request_id(), delay)

    def get_log_service(self, host: str, port: int) -> RequestReturn:
        return self._client.get_log_service(self._gen_request_id(),
                                            ServiceClient.get_log_service_path(host, port, str(self.id)))

    def start_stream(self, config: dict) -> RequestReturn:
        return self._client.start_stream(self._gen_request_id(), config)

    def stop_stream(self, stream_id: str) -> RequestReturn:
        return self._client.stop_stream(self._gen_request_id(), stream_id)

    def restart_stream(self, stream_id: str) -> RequestReturn:
        return self._client.restart_stream(self._gen_request_id(), stream_id)

    def get_log_stream(self, host: str, port: int, stream_id: str, feedback_directory: str) -> RequestReturn:
        return self._client.get_log_stream(self._gen_request_id(), stream_id, feedback_directory,
                                           ServiceClient.get_log_stream_path(host, port, stream_id))

    def get_pipeline_stream(self, host: str, port: int, stream_id: str, feedback_directory: str) -> RequestReturn:
        return self._client.get_pipeline_stream(self._gen_request_id(), stream_id, feedback_directory,
                                                ServiceClient.get_pipeline_stream_path(host, port, stream_id))

    def sync_service(self, streams_objects) -> RequestReturn:
        streams = []
        for streams_object in streams_objects:
            config = streams_object.config()
            streams.append(config)

        return self._client.sync_service(self._gen_request_id(), streams)

    def prepare_service(self, settings) -> RequestReturn:
        if not settings:
            return False, None

        return self._client.prepare_service(self._gen_request_id(), settings.feedback_directory,
                                            settings.timeshifts_directory,
                                            settings.hls_directory,
                                            settings.vods_directory, settings.cods_directory, settings.proxy_directory)

    @property
    def http_host(self) -> str:
        return self._http_host

    @property
    def os(self) -> OperationSystem:
        return self._os

    @property
    def vods_host(self) -> str:
        return self._vods_host

    @property
    def cods_host(self) -> str:
        return self._cods_host

    @property
    def project(self) -> str:
        return self._project

    @property
    def version(self) -> str:
        return self._version

    @property
    def exp_time(self):
        return self._exp_time

    # handler
    def process_response(self, client, req: Request, resp: Response):
        if not req:
            return

        if req.method == Commands.ACTIVATE_COMMAND and resp.is_message():
            if self._handler:
                result = resp.result

                os = OperationSystem(**result[ServiceClient.OS])

                self._set_runtime_fields(result[ServiceClient.HTTP_HOST], result[ServiceClient.VODS_HOST],
                                         result[ServiceClient.CODS_HOST], result[ServiceClient.PROJECT],
                                         result[ServiceClient.VERSION], os, result[ServiceClient.EXP_TIME])
                self._handler.on_service_statistic_received(result)

        if req.method == Commands.PREPARE_SERVICE_COMMAND and resp.is_message():
            pass

    def process_request(self, client, req: Request):
        if not req:
            return

        if not self._handler:
            return

        if req.method == Commands.STATISTIC_STREAM_COMMAND:
            assert req.is_notification()
            self._handler.on_stream_statistic_received(req.params)
        elif req.method == Commands.CHANGED_STREAM_COMMAND:
            assert req.is_notification()
            self._handler.on_stream_sources_changed(req.params)
        elif req.method == Commands.ML_NOTIFICATION_STREAM_COMMAND:
            assert req.is_notification()
            self._handler.on_stream_ml_notification(req.params)
        elif req.method == Commands.STATISTIC_SERVICE_COMMAND:
            assert req.is_notification()
            self._handler.on_service_statistic_received(req.params)
        elif req.method == Commands.QUIT_STATUS_STREAM_COMMAND:
            assert req.is_notification()
            self._handler.on_quit_status_stream(req.params)
        elif req.method == Commands.CLIENT_PING_COMMAND:
            self._handler.on_ping_received(req.params)

    def on_client_state_changed(self, client, status: ClientStatus):
        if status != ClientStatus.ACTIVE:
            self._set_runtime_fields()
        if self._handler:
            self._handler.on_client_state_changed(status)

    # private
    def _set_runtime_fields(self, http_host=None, vods_host=None, cods_host=None, project=None, version=None, os=None,
                            exp_time=None):
        self._http_host = http_host
        self._vods_host = vods_host
        self._cods_host = cods_host
        self._project = project
        self._version = version
        self._exp_time = exp_time
        self._os = os

    def _gen_request_id(self) -> int:
        current_value = self._request_id
        self._request_id += 1
        return current_value
