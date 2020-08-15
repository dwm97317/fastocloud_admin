from flask import session
from flask_login import UserMixin, login_user, logout_user

from pyfastocloud_models.provider.entry import Provider


class ProviderUser(UserMixin, Provider):
    SERVER_POSITION_SESSION_FIELD = 'server_position'

    def login(self):
        self.set_current_server_position(0)
        login_user(self)

    def logout(self):
        session.pop(ProviderUser.SERVER_POSITION_SESSION_FIELD)
        logout_user()

    def set_current_server_position(self, pos: int):
        session[ProviderUser.SERVER_POSITION_SESSION_FIELD] = pos

    def get_current_server(self):
        if not self.servers:
            return None

        server_settings = self.servers[session[ProviderUser.SERVER_POSITION_SESSION_FIELD]]
        if server_settings:
            from app import servers_manager
            return servers_manager.find_or_create_server(server_settings)

        return None
