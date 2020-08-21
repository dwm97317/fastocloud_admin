import logging
from datetime import datetime

import pyfastocloud_models.constants as constants
from app.service.service_client import ServiceClientPanel
from bson.objectid import ObjectId
from pyfastocloud.client_constants import ClientStatus, RequestReturn
from pyfastocloud.json_rpc import Response
from pyfastocloud_models.machine_entry import Machine
from pyfastocloud_models.series.entry import Serial
from pyfastocloud_models.service.entry import ServiceSettings, ProviderPair
from pyfastocloud_models.stream.entry import IStream
from pyfastocloud_models.utils.utils import date_to_utc_msec
from pyfastocloud_node.service.service_client import OnlineUsers
from pyfastocloud_node.service.stream_handler import IStreamHandler
from pyfastocloud_node.structs import OperationSystem

from app.service.stream import IStreamObject, ProxyStreamObject, ProxyVodStreamObject, RelayStreamObject, \
    VodRelayStreamObject, EncodeStreamObject, VodEncodeStreamObject, TimeshiftRecorderStreamObject, \
    TimeshiftPlayerStreamObject, CatchupStreamObject, EventStreamObject, CodEncodeStreamObject, CodRelayStreamObject, \
    TestLifeStreamObject


class ServiceFields:
    ID = 'id'
    PROJECT = 'project'
    VERSION = 'version'
    EXP_TIME = 'expiration_time'
    SYNCTIME = 'synctime'
    ONLINE_USERS = 'online_users'
    STATUS = 'status'
    OS = 'os'


class Service(IStreamHandler):
    SOCKETIO_NAMESPACE = '/service'

    SERVICE_DATA_CHANGED = 'service_data_changed'
    STREAM_ADDED = 'stream_added'
    STREAM_UPDATED = 'stream_updated'
    STREAM_REMOVED = 'stream_removed'
    STREAM_ML_NOTIFICATION = 'stream_ml_notification'
    SERIAL_ADDED = 'serial_added'
    SERIAL_UPDATED = 'serial_updated'
    SERIAL_REMOVED = 'serial_removed'
    SERVICE_RPC = 'rpc'
    OID = 'oid'
    PARAMS = 'params'

    DUMP_STATS_TIME_MSEC = 60000
    CALCULATE_VALUE = None

    # runtime
    _monitoring_timestamp = 0
    _machine = Machine.default()
    _sync_time = CALCULATE_VALUE
    _online_clients = OnlineUsers.default()
    _streams = []

    def __init__(self, socketio, settings: ServiceSettings):
        self._settings = settings
        # other fields
        self._client = ServiceClientPanel(settings.id, settings.host.host, settings.host.port, self)
        self._socketio = socketio
        self.__reload_from_db()

    def auto_start(self):
        if not self._settings.auto_start:
            return

        self.connect()
        if self._settings.activation_key:
            self.activate(self._settings.activation_key)

            for stream in self.streams:
                stream.auto_start()

    @property
    def settings(self):
        return self._settings

    def connect(self):
        return self._client.connect()

    def is_connected(self):
        return self._client.is_connected()

    def disconnect(self):
        return self._client.disconnect()

    def socket(self):
        return self._client.socket()

    def recv_data(self):
        return self._client.recv_data()

    def stop(self, delay: int) -> RequestReturn:
        return self._client.stop_service(delay)

    def get_log(self, route: str) -> RequestReturn:
        return self._client.get_log_service(route)

    def ping(self) -> RequestReturn:
        return self._client.ping_service()

    def activate(self, license_key: str) -> RequestReturn:
        return self._client.activate(license_key)

    def sync(self, prepare=False) -> RequestReturn:
        if prepare:
            self._client.prepare_service(self._settings.feedback_directory, self._settings.timeshifts_directory,
                                         self._settings.hls_directory, self._settings.vods_directory,
                                         self._settings.cods_directory, self._settings.proxy_directory)
        res, oid = self._client.sync_service(self._streams)
        self.__refresh_catchups()
        if res:
            self._sync_time = datetime.now()
        return res, oid

    def get_log_stream(self, sid: ObjectId, route: str) -> RequestReturn:
        stream = self.find_stream_by_id(sid)
        if not stream:
            return False, None

        return stream.get_log_request(route)

    def get_pipeline_stream(self, sid: ObjectId, route: str) -> RequestReturn:
        stream = self.find_stream_by_id(sid)
        if not stream:
            return False, None

        return stream.get_pipeline_request(route)

    def start_stream(self, sid: ObjectId) -> RequestReturn:
        stream = self.find_stream_by_id(sid)
        if not stream:
            return False, None

        return stream.start_request()

    def stop_stream(self, sid: ObjectId) -> RequestReturn:
        stream = self.find_stream_by_id(sid)
        if not stream:
            return False, None

        return stream.stop_request()

    def restart_stream(self, sid: ObjectId) -> RequestReturn:
        stream = self.find_stream_by_id(sid)
        if not stream:
            return False, None

        return stream.restart_request()

    def get_id(self):
        return str(self.id)

    @property
    def room(self) -> str:
        return str(self.id)

    @property
    def namespace(self) -> str:
        return '{0}_{1}'.format(Service.SOCKETIO_NAMESPACE, self.id)

    @property
    def id(self) -> ObjectId:
        return self._settings.id

    @property
    def status(self) -> ClientStatus:
        return self._client.status()

    @property
    def cpu(self):
        return self._machine.cpu

    @property
    def gpu(self):
        return self._machine.gpu

    @property
    def load_average(self):
        return self._machine.load_average

    @property
    def memory_total(self):
        return self._machine.memory_total

    @property
    def memory_free(self):
        return self._machine.memory_free

    @property
    def hdd_total(self):
        return self._machine.hdd_total

    @property
    def hdd_free(self):
        return self._machine.memory_free

    @property
    def bandwidth_in(self):
        return self._machine.bandwidth_in

    @property
    def bandwidth_out(self):
        return self._machine.bandwidth_out

    @property
    def uptime(self):
        return self._machine.uptime

    @property
    def synctime(self):
        if self._sync_time == Service.CALCULATE_VALUE:
            return Service.CALCULATE_VALUE
        return date_to_utc_msec(self._sync_time)

    @property
    def timestamp(self):
        return self._machine.timestamp

    @property
    def project(self) -> str:
        if self._client.project == 'fastocloud_pro':
            return 'FastoCloud PRO'
        elif self._client.project == 'fastocloud_pro_ml':
            return 'FastoCloud PRO ML'
        return 'FastoCloud'

    @property
    def version(self) -> str:
        return self._client.version

    @property
    def exp_time(self):
        return self._client.exp_time

    @property
    def os(self) -> OperationSystem:
        return self._client.os

    @property
    def online_users(self) -> OnlineUsers:
        return self._online_clients

    @property
    def streams(self):
        return self._streams

    def find_stream_by_id(self, sid: ObjectId) -> IStreamObject:
        for stream in self._streams:
            if stream.id == sid:
                return stream

        return None  #

    def generate_playlist(self):
        return self._settings.generate_playlist()

    def get_user_role_by_id(self, uid: ObjectId) -> ProviderPair.Roles:
        for user in self._settings.providers:
            if user.user.id == uid:
                return user.role

        return ProviderPair.Roles.READ

    def add_provider(self, provider: ProviderPair):
        self._settings.add_provider(provider)
        self._settings.save()

        provider.user.add_server(self._settings)
        provider.user.save()

    def remove_provider(self, provider):
        self._settings.remove_provider(provider)
        self._settings.save()

        provider.remove_server(self._settings)
        provider.save()

    def add_stream_by_id(self, sid: ObjectId) -> IStreamObject:
        stream = IStream.get_by_id(sid)
        return self.add_stream(stream)

    def get_series(self) -> [Serial]:
        return self._settings.series

    def add_serial(self, serial: Serial):
        if not serial:
            return None

        self._settings.add_serial(serial)
        self._settings.save()
        self._notify_serial_added(serial)
        return serial

    def update_serial(self, serial: Serial):
        if not serial:
            return None

        serial.save()
        # self._settings.refresh_from_db(['series'])
        self._notify_serial_updated(serial)

    def remove_serial(self, sid: ObjectId):
        serial = Serial.get_by_id(sid)
        if serial:
            self._settings.remove_serial(serial)
            self._settings.save()
            self._notify_serial_removed(serial)

    def add_stream(self, stream: IStream):
        if not stream:
            return None

        stream_object = self.__convert_stream(stream)
        self._streams.append(stream_object)
        self._settings.add_stream(stream)
        self._settings.save()
        self._notify_stream_added(stream_object)
        return stream_object

    def update_stream(self, stream: IStream):
        if not stream:
            return None

        stream.save(self._settings)
        stream_object = self.find_stream_by_id(stream.id)
        if not stream_object:
            return None

        stream_object._stream = stream
        self._notify_stream_updated(stream_object)

    def remove_stream(self, sid: ObjectId):
        copy = list(self._streams)
        for stream in copy:
            if stream.id == sid:
                stream.stop_request()
                self._streams.remove(stream)
                istream = stream.stream()
                self._settings.remove_stream(istream)
                self._notify_stream_removed(stream)
        self._settings.save()

    def remove_all_streams(self):
        copy = list(self._streams)
        self._streams = []
        for stream in copy:
            self._client.stop_stream(stream.get_id())
            self._notify_stream_removed(stream)
        self._settings.remove_all_streams()  #
        self._settings.save()

    def probe_stream(self, url: str) -> RequestReturn:
        return self._client.probe_stream(url)

    def scan_folder(self, directory: str, extensions: list) -> RequestReturn:
        return self._client.scan_folder(directory, extensions)

    def scan_folder_vods(self, directory: str, extensions: list, default_icon: str) -> RequestReturn:
        return self._client.scan_folder_vods(directory, extensions, default_icon)

    def stop_all_streams(self):
        for stream in self._streams:
            self._client.stop_stream(stream.get_id())

    def start_all_streams(self):
        for stream in self._streams:
            self._client.start_stream(stream.config())

    def to_front_dict(self) -> dict:
        settings_front = self._settings.to_front_dict()
        os = self.os.to_dict() if self.os else None

        settings_front[ServiceFields.VERSION] = self.version
        settings_front[ServiceFields.PROJECT] = self.project
        settings_front[ServiceFields.EXP_TIME] = self.exp_time
        settings_front[ServiceFields.SYNCTIME] = self.synctime
        settings_front[ServiceFields.STATUS] = self.status
        settings_front[ServiceFields.ONLINE_USERS] = self.online_users.to_dict()
        settings_front[ServiceFields.OS] = os
        return {**settings_front, **self._machine.to_front_dict()}

    def make_proxy_stream(self) -> ProxyStreamObject:
        return ProxyStreamObject.make_stream(self._settings)

    def make_proxy_vod(self) -> ProxyStreamObject:
        return ProxyVodStreamObject.make_stream(self._settings)

    def make_relay_stream(self) -> RelayStreamObject:
        return RelayStreamObject.make_stream(self._settings, self._client)

    def make_vod_relay_stream(self) -> VodRelayStreamObject:
        return VodRelayStreamObject.make_stream(self._settings, self._client)

    def make_cod_relay_stream(self) -> CodRelayStreamObject:
        return CodRelayStreamObject.make_stream(self._settings, self._client)

    def make_encode_stream(self) -> EncodeStreamObject:
        return EncodeStreamObject.make_stream(self._settings, self._client)

    def make_vod_encode_stream(self) -> VodEncodeStreamObject:
        return VodEncodeStreamObject.make_stream(self._settings, self._client)

    def make_event_stream(self) -> VodEncodeStreamObject:
        return EventStreamObject.make_stream(self._settings, self._client)

    def make_cod_encode_stream(self) -> CodEncodeStreamObject:
        return CodEncodeStreamObject.make_stream(self._settings, self._client)

    def make_timeshift_recorder_stream(self) -> TimeshiftRecorderStreamObject:
        return TimeshiftRecorderStreamObject.make_stream(self._settings, self._client)

    def make_catchup_stream(self) -> CatchupStreamObject:
        return CatchupStreamObject.make_stream(self._settings, self._client)

    def make_timeshift_player_stream(self) -> TimeshiftPlayerStreamObject:
        return TimeshiftPlayerStreamObject.make_stream(self._settings, self._client)

    def make_test_life_stream(self) -> TestLifeStreamObject:
        return TestLifeStreamObject.make_stream(self._settings, self._client)

    # handler
    def _notify_serial_added(self, serial: Serial):
        self.__notify_front(Service.SERIAL_ADDED, serial.to_front_dict())

    def _notify_serial_updated(self, serial: Serial):
        self.__notify_front(Service.SERIAL_UPDATED, serial.to_front_dict())

    def _notify_serial_removed(self, serial: Serial):
        self.__notify_front(Service.SERIAL_REMOVED, serial.to_front_dict())

    def _notify_stream_added(self, stream: IStreamObject):
        self.__notify_front(Service.STREAM_ADDED, stream.to_front_dict())

    def _notify_stream_updated(self, stream: IStreamObject):
        self.__notify_front(Service.STREAM_UPDATED, stream.to_front_dict())

    def _notify_stream_removed(self, stream: IStreamObject):
        self.__notify_front(Service.STREAM_REMOVED, stream.to_front_dict())

    def _notify_stream_ml_notification(self, stream: IStreamObject):
        self.__notify_front(Service.STREAM_ML_NOTIFICATION, stream.to_front_dict())

    def on_stream_statistic_received(self, params: dict):
        sid = params['id']
        stream = self.find_stream_by_id(ObjectId(sid))
        if stream:
            stream.update_runtime_fields(params)
            self._notify_stream_updated(stream)

    def on_stream_sources_changed(self, params: dict):
        pass

    def on_stream_ml_notification(self, params: dict):
        sid = params['id']
        stream = self.find_stream_by_id(ObjectId(sid))
        if stream:
            self._notify_stream_ml_notification(stream)

    def on_service_statistic_received(self, params: dict):
        # nid = params['id']
        self.__refresh_stats(params)
        self.__refresh_front()

    def on_quit_status_stream(self, params: dict):
        sid = params['id']
        stream = self.find_stream_by_id(ObjectId(sid))
        if stream:
            stream.reset()
            self._notify_stream_updated(stream)

    def on_client_state_changed(self, status: ClientStatus):
        if status == ClientStatus.ACTIVE:
            self.sync(True)
        else:
            self.__reset()
            for stream in self._streams:
                stream.reset()
        self.__refresh_front()

    def on_ping_received(self, params: dict):
        self.sync()

    def on_probe_stream(self, rpc: Response):
        self.__notify_front(Service.SERVICE_RPC, rpc.to_dict())

    def on_scan_folder(self, rpc: Response):
        self.__notify_front(Service.SERVICE_RPC, rpc.to_dict())

    def on_scan_folder_vods(self, rpc: Response):
        self.__notify_front(Service.SERVICE_RPC, rpc.to_dict())

    # private
    def __refresh_front(self):
        self.__notify_front(Service.SERVICE_DATA_CHANGED, self.to_front_dict())

    def __notify_front(self, channel: str, json: dict):
        self._socketio.emit(channel, json, namespace=self.namespace)

    def __reset(self):
        self._machine = Machine.default()
        self._sync_time = Service.CALCULATE_VALUE
        self._online_clients = OnlineUsers.default()

    def __refresh_stats(self, stats: dict):
        self._machine = Machine.make_entry(stats)
        self._online_clients = OnlineUsers(**stats[ServiceFields.ONLINE_USERS])
        if self._settings.monitoring:
            curts = self._machine.timestamp
            need_shot = (curts - self._monitoring_timestamp) > Service.DUMP_STATS_TIME_MSEC
            if need_shot:
                self._settings.add_stat(self._machine)
                self._settings.save()
                self._monitoring_timestamp = curts

    def __reload_from_db(self):
        self._streams = []
        settings = self._settings
        for i, stream in enumerate(settings.streams):
            if stream:
                stream_object = self.__convert_stream(stream)
                if stream_object:
                    self._streams.append(stream_object)
                else:
                    logging.warning('Failed to create stream object: %s', stream.get_id())
            else:
                logging.warning('Invalid stream in service: %s, position: %d', settings.get_id(), i)

    def __refresh_catchups(self):
        for stream in self._streams:
            if stream.type == constants.StreamType.CATCHUP:
                stream.start_request()

    def __convert_stream(self, stream: IStream) -> IStreamObject:
        stream_type = stream.get_type()
        if stream_type == constants.StreamType.PROXY:
            return ProxyStreamObject(stream, self._settings)
        elif stream_type == constants.StreamType.VOD_PROXY:
            return ProxyVodStreamObject(stream, self._settings)
        elif stream_type == constants.StreamType.RELAY:
            return RelayStreamObject(stream, self._settings, self._client)
        elif stream_type == constants.StreamType.ENCODE:
            return EncodeStreamObject(stream, self._settings, self._client)
        elif stream_type == constants.StreamType.TIMESHIFT_PLAYER:
            return TimeshiftPlayerStreamObject(stream, self._settings, self._client)
        elif stream_type == constants.StreamType.TIMESHIFT_RECORDER:
            return TimeshiftRecorderStreamObject(stream, self._settings, self._client)
        elif stream_type == constants.StreamType.CATCHUP:
            return CatchupStreamObject(stream, self._settings, self._client)
        elif stream_type == constants.StreamType.TEST_LIFE:
            return TestLifeStreamObject(stream, self._settings, self._client)
        elif stream_type == constants.StreamType.VOD_RELAY:
            return VodRelayStreamObject(stream, self._settings, self._client)
        elif stream_type == constants.StreamType.VOD_ENCODE:
            return VodEncodeStreamObject(stream, self._settings, self._client)
        elif stream_type == constants.StreamType.COD_RELAY:
            return CodRelayStreamObject(stream, self._settings, self._client)
        elif stream_type == constants.StreamType.COD_ENCODE:
            return CodEncodeStreamObject(stream, self._settings, self._client)
        elif stream_type == constants.StreamType.EVENT:
            return EventStreamObject(stream, self._settings, self._client)
        else:
            return None  #
