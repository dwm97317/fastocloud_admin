import os

from flask import Flask
from flask_bootstrap import Bootstrap
from flask_login import LoginManager
from flask_mail import Mail
from flask_pymodm import PyModm
from flask_socketio import SocketIO
from werkzeug.middleware.proxy_fix import ProxyFix

from app.service.service_manager import ServiceManager


def get_app_folder():
    return os.path.dirname(__file__)


def get_runtime_folder():
    return os.path.join(get_app_folder(), 'runtime_folder')


def get_runtime_stream_folder():
    return os.path.join(get_runtime_folder(), 'stream')


def get_epg_tmp_folder():
    return os.path.join(get_runtime_folder(), 'epg')


def init_project(static_folder, *args):
    runtime_folder = get_runtime_folder()
    if not os.path.exists(runtime_folder):
        os.mkdir(runtime_folder)

    runtime_stream_folder = get_runtime_stream_folder()
    if not os.path.exists(runtime_stream_folder):
        os.mkdir(runtime_stream_folder)

    epg_tmp_folder = get_epg_tmp_folder()
    if not os.path.exists(epg_tmp_folder):
        os.mkdir(epg_tmp_folder)

    _app = Flask(__name__, static_folder=static_folder)
    for file in args:
        _app.config.from_pyfile(file, silent=False)

    _app.wsgi_app = ProxyFix(_app.wsgi_app)
    Bootstrap(_app)
    _db = PyModm(_app)
    _mail = Mail(_app)
    _socketio = SocketIO(_app, async_mode='gevent')
    _login_manager = LoginManager(_app)

    _login_manager.login_view = 'HomeView:signin'

    # socketio
    @_socketio.on('connect')
    def connect():
        pass

    @_socketio.on('disconnect')
    def disconnect():
        pass

    # defaults flask
    _host = '0.0.0.0'
    _port = 8080
    server_name = _app.config.get('SERVER_NAME_FOR_POST')
    sn_host, sn_port = None, None

    if server_name:
        sn_host, _, sn_port = server_name.partition(':')

    host = sn_host or _host
    port = int(sn_port or _port)
    _servers_manager = ServiceManager(host, port, _socketio)

    return _app, _mail, _login_manager, _servers_manager, _db


app, mail, login_manager, servers_manager, db = init_project(
    'static',
    'config/public_config.py',
    'config/config.py',
    'config/db_config.py',
    'config/mail_config.py'
)

from app.home.view import HomeView
from app.provider.view import ProviderView
from app.stream.view import StreamView
from app.service.view import ServiceView

HomeView.register(app)
ProviderView.register(app)
StreamView.register(app)
ServiceView.register(app)
