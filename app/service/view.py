import logging
import os

import pyfastocloud_models.constants as constants
from bson.objectid import ObjectId
from flask import render_template, redirect, url_for, request, jsonify, Response
from flask_classy import FlaskView, route
from flask_login import login_required, current_user
from pyfastocloud_models.provider.entry_pair import ProviderPair
from pyfastocloud_models.service.entry import ServiceSettings
from pyfastocloud_models.utils.m3u_parser import M3uParser
from pyfastocloud_models.utils.utils import is_valid_http_url, is_valid_url

from app import get_runtime_folder
from app.common.service.forms import ServiceSettingsForm, ActivateForm, UploadM3uForm, ServerProviderForm
from app.home.entry import ProviderUser


# routes
class ServiceView(FlaskView):
    route_base = "/service/"

    @login_required
    @route('/upload_m3u', methods=['POST', 'GET'])
    def upload_m3u(self):
        form = UploadM3uForm()
        return render_template('service/upload_m3u.html', form=form)

    @login_required
    @route('/upload_files', methods=['POST'])
    def upload_files(self):
        form = UploadM3uForm()
        server = current_user.get_current_server()
        if server and form.validate_on_submit():
            stream_type = form.type.data
            files = request.files.getlist("files")
            for file in files:
                m3u_parser = M3uParser()
                data = file.read().decode('utf-8')
                m3u_parser.load_content(data)
                m3u_parser.parse()

                streams = []
                for mfile in m3u_parser.files:
                    input_url = mfile['link']
                    if not is_valid_url(input_url):
                        logging.warning('Skipped invalid url: %s', input_url)
                        continue

                    if stream_type == constants.StreamType.PROXY:
                        stream_object = server.make_proxy_stream()
                        stream = stream_object.stream()
                    elif stream_type == constants.StreamType.VOD_PROXY:
                        stream_object = server.make_proxy_vod()
                        stream = stream_object.stream()
                    elif stream_type == constants.StreamType.RELAY:
                        stream_object = server.make_relay_stream()
                        stream = stream_object.stream()
                        sid = stream.output[0].id
                        stream.output = [stream_object.generate_http_link(constants.HlsType.HLS_PULL, oid=sid)]
                    elif stream_type == constants.StreamType.ENCODE:
                        stream_object = server.make_encode_stream()
                        stream = stream_object.stream()
                        sid = stream.output[0].id
                        stream.output = [stream_object.generate_http_link(constants.HlsType.HLS_PULL, oid=sid)]
                    elif stream_type == constants.StreamType.VOD_RELAY:
                        stream_object = server.make_vod_relay_stream()
                        stream = stream_object.stream()
                        sid = stream.output[0].id
                        stream.output = [stream_object.generate_vod_link(constants.HlsType.HLS_PULL, oid=sid)]
                    elif stream_type == constants.StreamType.VOD_ENCODE:
                        stream_object = server.make_vod_encode_stream()
                        stream = stream_object.stream()
                        sid = stream.output[0].id
                        stream.output = [stream_object.generate_vod_link(constants.HlsType.HLS_PULL, oid=sid)]
                    elif stream_type == constants.StreamType.COD_RELAY:
                        stream_object = server.make_cod_relay_stream()
                        stream = stream_object.stream()
                        sid = stream.output[0].id
                        stream.output = [stream_object.generate_cod_link(constants.HlsType.HLS_PULL, oid=sid)]
                    elif stream_type == constants.StreamType.COD_ENCODE:
                        stream_object = server.make_cod_encode_stream()
                        stream = stream_object.stream()
                        sid = stream.output[0].id
                        stream.output = [stream_object.generate_cod_link(constants.HlsType.HLS_PULL, oid=sid)]
                    elif stream_type == constants.StreamType.CATCHUP:
                        stream_object = server.make_catchup_stream()
                        stream = stream_object.stream()
                    else:
                        stream_object = server.make_test_life_stream()
                        stream = stream_object.stream()

                    if stream_type == constants.StreamType.PROXY or stream_type == constants.StreamType.VOD_PROXY:
                        stream.output[0].uri = input_url
                    else:
                        stream.input[0].uri = input_url

                    title = mfile['title']
                    if len(title) < constants.MAX_STREAM_NAME_LENGTH:
                        stream.name = title

                    tvg_id = mfile['tvg-id']
                    if tvg_id and len(tvg_id) < constants.MAX_STREAM_TVG_ID_LENGTH:
                        stream.tvg_id = tvg_id

                    tvg_name = mfile['tvg-name']
                    if tvg_name and len(tvg_name) < constants.MAX_STREAM_NAME_LENGTH:
                        stream.tvg_name = tvg_name

                    tvg_group = mfile['tvg-group']
                    if tvg_group:
                        stream.groups = [tvg_group]

                    tvg_logo = mfile['tvg-logo']
                    if tvg_logo and len(tvg_logo) < constants.MAX_URI_LENGTH:
                        if is_valid_http_url(tvg_logo, timeout=0.05):
                            stream.tvg_logo = tvg_logo

                    is_valid_stream = stream.is_valid()
                    if is_valid_stream:
                        stream.save(server.settings)
                        streams.append(stream)

                server.add_streams(streams)

        return redirect(url_for('ProviderView:dashboard'))

    @login_required
    def connect(self):
        server = current_user.get_current_server()
        if server:
            server.connect()
        return redirect(url_for('ProviderView:dashboard'))

    @login_required
    def disconnect(self):
        server = current_user.get_current_server()
        if server:
            server.disconnect()
        return redirect(url_for('ProviderView:dashboard'))

    @route('/activate', methods=['POST', 'GET'])
    @login_required
    def activate(self):
        form = ActivateForm()
        if request.method == 'POST':
            server = current_user.get_current_server()
            if server:
                if form.validate_on_submit():
                    lic = form.license.data
                    server.activate(lic)
                    return redirect(url_for('ProviderView:dashboard'))

        return render_template('service/activate.html', form=form)

    @login_required
    def sync(self):
        server = current_user.get_current_server()
        if server:
            server.sync()
        return redirect(url_for('ProviderView:dashboard'))

    @login_required
    def stop(self):
        server = current_user.get_current_server()
        if server:
            server.stop(1)
        return redirect(url_for('ProviderView:dashboard'))

    @login_required
    def ping(self):
        server = current_user.get_current_server()
        if server:
            server.ping()
        return redirect(url_for('ProviderView:dashboard'))

    @login_required
    def get_log(self):
        server = current_user.get_current_server()
        if server:
            server.get_log_service()
        return redirect(url_for('ProviderView:dashboard'))

    @login_required
    @route('/playlist/<sid>/master.m3u', methods=['GET'])
    def playlist(self, sid):
        server = ServiceSettings.get_by_id(ObjectId(sid))
        if server:
            return Response(server.generate_playlist(), mimetype='application/x-mpequrl'), 200

        return jsonify(status='failed'), 404

    @login_required
    def view_log(self):
        server = current_user.get_current_server()
        if server:
            path = os.path.join(get_runtime_folder(), str(server.id))
            try:
                with open(path, "r") as f:
                    content = f.read()

                return content
            except OSError as e:
                print('Caught exception OSError : {0}'.format(e))
                return '''<pre>Not found, please use get log button firstly.</pre>'''
        return '''<pre>Not found, please create server firstly.</pre>'''

    # broadcast routes

    @login_required
    def providers(self, sid):
        server = ServiceSettings.get_by_id(ObjectId(sid))
        if server:
            return render_template('service/providers.html', server=server)

        return redirect(url_for('ProviderView:dashboard'))

    @login_required
    @route('/provider/add/<sid>', methods=['GET', 'POST'])
    def provider_add(self, sid):
        form = ServerProviderForm()
        if request.method == 'POST' and form.validate_on_submit():
            email = form.email.data.lower()
            provider = ProviderUser.get_by_email(email)
            server = ServiceSettings.get_by_id(ObjectId(sid))
            if server and provider:
                admin = ProviderPair(provider.id, form.role.data)
                server.add_provider(admin)
                server.save()

                provider.add_server(server)
                provider.save()
                return jsonify(status='ok'), 200

        return render_template('service/provider/add.html', form=form)

    @login_required
    @route('/provider/remove/<sid>', methods=['POST'])
    def provider_remove(self, sid):
        data = request.get_json()
        pid = data['pid']
        provider = ProviderUser.get_by_id(ObjectId(pid))
        server = ServiceSettings.get_by_id(ObjectId(sid))
        if provider and server:
            server.remove_provider(provider)
            server.save()

            provider.remove_server(server)
            provider.save()
            return jsonify(status='ok'), 200

        return jsonify(status='failed'), 404

    @login_required
    @route('/add', methods=['GET', 'POST'])
    def add(self):
        form = ServiceSettingsForm(obj=ServiceSettings())
        if request.method == 'POST' and form.validate_on_submit():
            new_entry = form.make_entry()
            admin = ProviderPair(user=current_user.id, role=ProviderPair.Roles.ADMIN)
            new_entry.add_provider(admin)
            new_entry.save()

            current_user.add_server(new_entry)
            current_user.save()
            return jsonify(status='ok'), 200

        return render_template('service/add.html', form=form)

    @login_required
    @route('/remove', methods=['POST'])
    def remove(self):
        sid = request.form['sid']
        server = ServiceSettings.get_by_id(ObjectId(sid))
        if server:
            current_user.set_current_server_position(0)
            server.delete()
            return jsonify(status='ok'), 200

        return jsonify(status='failed'), 404

    @login_required
    @route('/edit/<sid>', methods=['GET', 'POST'])
    def edit(self, sid):
        server = ServiceSettings.get_by_id(ObjectId(sid))
        form = ServiceSettingsForm(obj=server)

        if request.method == 'POST' and form.validate_on_submit():
            server = form.update_entry(server)
            server.save()
            return jsonify(status='ok'), 200

        return render_template('service/edit.html', form=form)

    @route('/log/<sid>', methods=['POST'])
    def log(self, sid):
        # len = request.headers['content-length']
        new_file_path = os.path.join(get_runtime_folder(), sid)
        with open(new_file_path, 'wb') as f:
            data = request.stream.read()
            f.write(b'<pre>')
            f.write(data)
            f.write(b'</pre>')
            f.close()
        return jsonify(status='ok'), 200
