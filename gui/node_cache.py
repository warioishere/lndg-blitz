from datetime import timedelta
from django.utils import timezone
from google.protobuf.json_format import MessageToDict, ParseDict

from .models import NodeCache, LocalSettings
from gui.lnd_deps import lightning_pb2 as ln


def get_node_info_cached(pubkey, stub, expiry_minutes=60):
    """Return node info using a simple database cache."""
    setting = LocalSettings.objects.filter(key='NODE_CACHE_EXPIRY_MINUTES').first()
    if setting:
        try:
            expiry_minutes = int(setting.value)
        except ValueError:
            pass
    cutoff = timezone.now() - timedelta(minutes=expiry_minutes)
    cache = NodeCache.objects.filter(pubkey=pubkey).first()
    if not cache or cache.updated_at < cutoff:
        try:
            info = stub.GetNodeInfo(ln.NodeInfoRequest(pub_key=pubkey, include_channels=False))
            NodeCache.objects.update_or_create(
                pubkey=pubkey,
                defaults={'data': MessageToDict(info), 'updated_at': timezone.now()},
            )
            return info
        except Exception:
            if cache:
                return ParseDict(cache.data, ln.NodeInfo())
            raise
    else:
        return ParseDict(cache.data, ln.NodeInfo())

