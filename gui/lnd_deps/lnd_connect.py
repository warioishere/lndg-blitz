import os, codecs, grpc
from lndg import settings

def get_creds():
    #Open connection with lnd via grpc
    with open(os.path.expanduser(settings.LND_MACAROON_PATH), 'rb') as f:
        macaroon_bytes = f.read()
        macaroon = codecs.encode(macaroon_bytes, 'hex')
    def metadata_callback(context, callback):
        callback([('macaroon', macaroon)], None)
    os.environ["GRPC_SSL_CIPHER_SUITES"] = 'HIGH+ECDSA'
    cert = open(os.path.expanduser(settings.LND_TLS_PATH), 'rb').read()
    cert_creds = grpc.ssl_channel_credentials(cert)
    auth_creds = grpc.metadata_call_credentials(metadata_callback)
    creds = grpc.composite_channel_credentials(cert_creds, auth_creds)
    return creds

creds = get_creds()

_channel = None
_async_channel = None

def lnd_connect():
    return grpc.secure_channel(
        settings.LND_RPC_SERVER,
        creds,
        options=[
            ("grpc.max_send_message_length", int(settings.LND_MAX_MESSAGE) * 1000000),
            ("grpc.max_receive_message_length", int(settings.LND_MAX_MESSAGE) * 1000000),
        ],
    )

def async_lnd_connect():
    return grpc.aio.secure_channel(
        settings.LND_RPC_SERVER,
        creds,
        options=[
            ("grpc.max_send_message_length", int(settings.LND_MAX_MESSAGE) * 1000000),
            ("grpc.max_receive_message_length", int(settings.LND_MAX_MESSAGE) * 1000000),
        ],
    )

def get_shared_channel():
    """Return a module-level grpc channel, creating it if necessary."""
    global _channel
    if _channel is None:
        _channel = lnd_connect()
    return _channel

def get_shared_async_channel():
    """Return a module-level asynchronous grpc channel."""
    global _async_channel
    if _async_channel is None:
        _async_channel = async_lnd_connect()
    return _async_channel

def close_shared_channel():
    """Close and reset the shared grpc channel."""
    global _channel
    if _channel is not None:
        _channel.close()
        _channel = None

def close_shared_async_channel():
    """Close and reset the shared asynchronous grpc channel."""
    global _async_channel
    if _async_channel is not None:
        try:
            _async_channel.close()
        finally:
            _async_channel = None

def main():
    pass

if __name__ == '__main__':
    main()