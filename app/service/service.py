from datetime import datetime

import pyfastocloud_models.constants as constants
from bson.objectid import ObjectId
from pyfastocloud.client_constants import ClientStatus
from pyfastocloud_models.provider.entry_pair import ProviderPair
from pyfastocloud_models.service.entry import ServiceSettings
from pyfastocloud_models.stream.entry import IStream
from pyfastocloud_models.utils.utils import date_to_utc_msec

from app.service.service_client import ServiceClient, OperationSystem, RequestReturn
from app.service.stream import IStreamObject, ProxyStreamObject, ProxyVodStreamObject, RelayStreamObject, \
    VodRelayStreamObject, EncodeStreamObject, VodEncodeStreamObject, TimeshiftRecorderStreamObject, \
    TimeshiftPlayerStreamObject, CatchupStreamObject, EventStreamObject, CodEncodeStreamObject, CodRelayStreamObject, \
    TestLifeStreamObject
from app.service.stream_handler import IStreamHandler


class OnlineUsers(object):
    __slots__ = ['daemon', 'http', 'vods', 'cods', 'subscribers']

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if key in self.__slots__:
                setattr(self, key, value)

    def __str__(self):
        if hasattr(self, 'subscribers'):
            return 'daemon:{0} http:{1} vods:{2} cods:{3} subscribers:{4}'.format(self.daemon, self.http, self.vods,
                                                                                  self.cods, self.subscribers)

        return 'daemon:{0} http:{1} vods:{2} cods:{3}'.format(self.daemon, self.http, self.vods, self.cods)


class ServiceFields:
    ID = 'id'
    CPU = 'cpu'
    GPU = 'gpu'
    LOAD_AVERAGE = 'load_average'
    MEMORY_TOTAL = 'memory_total'
    MEMORY_FREE = 'memory_free'
    HDD_TOTAL = 'hdd_total'
    HDD_FREE = 'hdd_free'
    BANDWIDTH_IN = 'bandwidth_in'
    BANDWIDTH_OUT = 'bandwidth_out'
    PROJECT = 'project'
    VERSION = 'version'
    EXP_TIME = 'exp_time'
    UPTIME = 'uptime'
    SYNCTIME = 'synctime'
    TIMESTAMP = 'timestamp'
    STATUS = 'status'
    ONLINE_USERS = 'online_users'
    OS = 'os'


class Service(IStreamHandler):
    SERVER_ID = 'server_id'
    STREAM_DATA_CHANGED = 'stream_data_changed'
    SERVICE_DATA_CHANGED = 'service_data_changed'
    INIT_VALUE = 0
    CALCULATE_VALUE = None

    # runtime
    _cpu = INIT_VALUE
    _gpu = INIT_VALUE
    _load_average = CALCULATE_VALUE
    _memory_total = INIT_VALUE
    _memory_free = INIT_VALUE
    _hdd_total = INIT_VALUE
    _hdd_free = INIT_VALUE
    _bandwidth_in = INIT_VALUE
    _bandwidth_out = INIT_VALUE
    _uptime = CALCULATE_VALUE
    _sync_time = CALCULATE_VALUE
    _timestamp = CALCULATE_VALUE
    _streams = []
    _online_users = None
    _os = OperationSystem()

    def __init__(self, host, port, socketio, settings: ServiceSettings):
        self._settings = settings
        # other fields
        self._client = ServiceClient(settings.id, settings.host.host, settings.host.port, self)
        self._host = host
        self._port = port
        self._socketio = socketio
        self.__reload_from_db()

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

    def get_log_service(self) -> RequestReturn:
        return self._client.get_log_service(self._host, self._port)

    def ping(self) -> RequestReturn:
        return self._client.ping_service()

    def activate(self, license_key: str) -> RequestReturn:
        return self._client.activate(license_key)

    def sync(self, prepare=False) -> RequestReturn:
        if prepare:
            self._client.prepare_service(self._settings)
        res, seq = self._client.sync_service(self._streams)
        self.__refresh_catchups()
        if res:
            self._sync_time = datetime.now()
        return res, seq

    def get_log_stream(self, sid: ObjectId):
        stream = self.find_stream_by_id(sid)
        if stream:
            stream.get_log_request(self._host, self._port)

    def get_pipeline_stream(self, sid: ObjectId):
        stream = self.find_stream_by_id(sid)
        if stream:
            stream.get_pipeline_request(self._host, self._port)

    def start_stream(self, sid: ObjectId):
        stream = self.find_stream_by_id(sid)
        if stream:
            stream.start_request()

    def stop_stream(self, sid: ObjectId):
        stream = self.find_stream_by_id(sid)
        if stream:
            stream.stop_request()

    def restart_stream(self, sid: ObjectId):
        stream = self.find_stream_by_id(sid)
        if stream:
            stream.restart_request()

    @property
    def settings(self) -> ServiceSettings:
        return self._settings

    @property
    def host(self) -> str:
        return self._host

    @property
    def id(self) -> ObjectId:
        return self._settings.id

    @property
    def status(self) -> ClientStatus:
        return self._client.status()

    @property
    def cpu(self):
        return self._cpu

    @property
    def gpu(self):
        return self._gpu

    @property
    def load_average(self):
        return self._load_average

    @property
    def memory_total(self):
        return self._memory_total

    @property
    def memory_free(self):
        return self._memory_free

    @property
    def hdd_total(self):
        return self._hdd_total

    @property
    def hdd_free(self):
        return self._hdd_free

    @property
    def bandwidth_in(self):
        return self._bandwidth_in

    @property
    def bandwidth_out(self):
        return self._bandwidth_out

    @property
    def uptime(self):
        return self._uptime

    @property
    def synctime(self):
        if not self._sync_time:
            return None
        return date_to_utc_msec(self._sync_time)

    @property
    def timestamp(self):
        return self._timestamp

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
        return self._online_users

    def get_streams(self):
        return self._streams

    def find_stream_by_id(self, sid: ObjectId) -> IStreamObject:
        for stream in self._streams:
            if stream.id == sid:
                return stream

        return None  #

    def get_user_role_by_id(self, uid: ObjectId) -> ProviderPair.Roles:
        for user in self._settings.providers:
            if user.user.id == uid:
                return user.role

        return ProviderPair.Roles.READ

    def add_stream(self, stream: IStream):
        if stream:
            stream_object = self.__convert_stream(stream)
            self._streams.append(stream_object)
            self._settings.add_stream(stream)
            self._settings.save()

    def add_streams(self, streams: [IStream]):
        stabled_streams = []
        for stream in streams:
            if stream:
                stream_object = self.__convert_stream(stream)
                self._streams.append(stream_object)
                stabled_streams.append(stream)

        self._settings.add_streams(stabled_streams)  #
        self._settings.save()

    def update_stream(self, stream: IStream):
        stream.save(self.settings)

    def remove_stream(self, sid: ObjectId):
        for stream in list(self._streams):
            if stream.id == sid:
                original = stream.stream()
                for part in list(original.parts):
                    self.remove_stream(part.id)

                stream.stop_request()
                self._streams.remove(stream)
                self._settings.remove_stream(original)
        self._settings.save()

    def remove_all_streams(self):
        for stream in self._streams:
            self._client.stop_stream(stream.get_id())
        self._streams = []
        self._settings.remove_all_streams()  #
        self._settings.save()

    def stop_all_streams(self):
        for stream in self._streams:
            self._client.stop_stream(stream.get_id())

    def start_all_streams(self):
        for stream in self._streams:
            self._client.start_stream(stream.config())

    def to_dict(self) -> dict:
        return {ServiceFields.ID: str(self.id), ServiceFields.CPU: self._cpu, ServiceFields.GPU: self._gpu,
                ServiceFields.LOAD_AVERAGE: self._load_average, ServiceFields.MEMORY_TOTAL: self._memory_total,
                ServiceFields.MEMORY_FREE: self._memory_free, ServiceFields.HDD_TOTAL: self._hdd_total,
                ServiceFields.HDD_FREE: self._hdd_free, ServiceFields.BANDWIDTH_IN: self._bandwidth_in,
                ServiceFields.BANDWIDTH_OUT: self._bandwidth_out, ServiceFields.PROJECT: self.project,
                ServiceFields.VERSION: self.version, ServiceFields.EXP_TIME: self.exp_time,
                ServiceFields.UPTIME: self._uptime, ServiceFields.SYNCTIME: self.synctime,
                ServiceFields.TIMESTAMP: self._timestamp, ServiceFields.STATUS: self.status,
                ServiceFields.ONLINE_USERS: str(self.online_users), ServiceFields.OS: str(self.os)}

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
    def on_stream_statistic_received(self, params: dict):
        sid = params['id']
        stream = self.find_stream_by_id(ObjectId(sid))
        if stream:
            stream.update_runtime_fields(params)
            self.__notify_front(Service.STREAM_DATA_CHANGED, stream.to_front_dict())

    def on_stream_sources_changed(self, params: dict):
        pass

    def on_stream_ml_notification(self, params: dict):
        pass

    def on_service_statistic_received(self, params: dict):
        # nid = params['id']
        self.__refresh_stats(params)
        self.__notify_front(Service.SERVICE_DATA_CHANGED, self.to_dict())

    def on_quit_status_stream(self, params: dict):
        sid = params['id']
        stream = self.find_stream_by_id(ObjectId(sid))
        if stream:
            stream.reset()
            self.__notify_front(Service.STREAM_DATA_CHANGED, stream.to_front_dict())

    def on_client_state_changed(self, status: ClientStatus):
        if status == ClientStatus.ACTIVE:
            self.sync(True)
        else:
            self.__reset()
            for stream in self._streams:
                stream.reset()

    def on_ping_received(self, params: dict):
        self.sync()

    # private
    def __notify_front(self, channel: str, params: dict):
        unique_channel = channel + '_' + str(self.id)
        self._socketio.emit(unique_channel, params)

    def __reset(self):
        self._cpu = Service.INIT_VALUE
        self._gpu = Service.INIT_VALUE
        self._load_average = Service.CALCULATE_VALUE
        self._memory_total = Service.INIT_VALUE
        self._memory_free = Service.INIT_VALUE
        self._hdd_total = Service.INIT_VALUE
        self._hdd_free = Service.INIT_VALUE
        self._bandwidth_in = Service.INIT_VALUE
        self._bandwidth_out = Service.INIT_VALUE
        self._uptime = Service.CALCULATE_VALUE
        self._sync_time = Service.CALCULATE_VALUE
        self._timestamp = Service.CALCULATE_VALUE
        self._online_users = None

    def __refresh_stats(self, stats: dict):
        self._cpu = stats[ServiceFields.CPU]
        self._gpu = stats[ServiceFields.GPU]
        self._load_average = stats[ServiceFields.LOAD_AVERAGE]
        self._memory_total = stats[ServiceFields.MEMORY_TOTAL]
        self._memory_free = stats[ServiceFields.MEMORY_FREE]
        self._hdd_total = stats[ServiceFields.HDD_TOTAL]
        self._hdd_free = stats[ServiceFields.HDD_FREE]
        self._bandwidth_in = stats[ServiceFields.BANDWIDTH_IN]
        self._bandwidth_out = stats[ServiceFields.BANDWIDTH_OUT]
        self._uptime = stats[ServiceFields.UPTIME]
        self._timestamp = stats[ServiceFields.TIMESTAMP]
        self._online_users = OnlineUsers(**stats[ServiceFields.ONLINE_USERS])

    def __reload_from_db(self):
        self._streams = []
        for stream in self._settings.streams:
            stream_object = self.__convert_stream(stream)
            if stream_object:
                self._streams.append(stream_object)

    def __refresh_catchups(self):
        self._settings.refresh_from_db()
        # FIXME workaround, need to listen load balance
        for stream in self._settings.streams:
            if stream and stream.get_type() == constants.StreamType.CATCHUP:
                if not self.find_stream_by_id(stream.pk):
                    stream_object = self.__convert_stream(stream)
                    if stream_object:
                        self._streams.append(stream_object)

        for stream in self._streams:
            if stream.type == constants.StreamType.CATCHUP:
                stream.start_request()

    def __convert_stream(self, stream: IStream) -> IStreamObject:
        if not stream:
            return

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
