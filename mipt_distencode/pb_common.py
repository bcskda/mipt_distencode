import logging

import grpc

from mipt_distencode.config import Config


ROOT_CERTCHAIN_PATH = 'config/ca.pem'
PRIVKEY_PATH = f'config/{Config.identity}-private.pem'
CERTCHAIN_PATH = f'config/{Config.identity}-cert.pem'


def _load_pems():
    logging.debug('Loading CA PEM: %s', ROOT_CERTCHAIN_PATH)
    with open(ROOT_CERTCHAIN_PATH, 'rb') as f:
        ca = f.read()
    logging.debug('Loading private key PEM: %s', PRIVKEY_PATH)
    with open(PRIVKEY_PATH, 'rb') as f:
        private = f.read()
    logging.debug('Loading certificate PEM: %s', CERTCHAIN_PATH)
    with open(CERTCHAIN_PATH, 'rb') as f:
        cert = f.read()
    return ca, private, cert


def load_channel_creds() -> grpc.ChannelCredentials:
    ca, private, cert = _load_pems()
    return grpc.ssl_channel_credentials(
        private_key=private,
        certificate_chain=cert,
        root_certificates=ca)


def load_server_creds() -> grpc.ServerCredentials:
    ca, private, cert = _load_pems()
    return grpc.ssl_server_credentials(
        private_key_certificate_chain_pairs=[(private, cert)],
        root_certificates=ca,
        require_client_auth=True)


def make_channel(endpoint, secure=False, creds=None) -> grpc.Channel:
    if secure:
        if creds is None:
            creds = load_channel_creds()
        return grpc.secure_channel(endpoint, creds)
    else:
        return grpc.insecure_channel(endpoint)


def add_endpoint_to_server(server, endpoint, secure=False, creds=None):
    if secure:
        if creds is None:
            creds = load_server_creds()
        server.add_secure_port(endpoint, creds)
    else:
        server.add_insecure_port(endpoint)


class PeerIdentityMixin:
    ALLOWED_SECURITY_LEVEL = [b'TSI_PRIVACY_AND_INTEGRITY']

    def identify_peer(self, pb_context) -> str:
        auth_ctx = pb_context.auth_context()
        if auth_ctx['security_level'] != self.ALLOWED_SECURITY_LEVEL:
            self.logger.error('Cannot proceed without peer identification')
            context.abort(
                grpc.StatusCode.UNAUTHENTICATED, 'Insufficient security_level')
        return auth_ctx['x509_common_name'][0].decode()
