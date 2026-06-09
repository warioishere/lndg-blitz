from datetime import timedelta
from collections import OrderedDict

from django.utils import timezone
from google.protobuf.json_format import MessageToDict, ParseDict

from .models import NodeCache, LocalSettings
from gui.lnd_deps import lightning_pb2 as ln

_memory_cache = OrderedDict()


def get_node_info_cached(pubkey, stub, expiry_minutes=60, max_entries=500):
    """Return node info using a combined in-memory and database cache."""
    expiry_setting = LocalSettings.objects.filter(key='NODE_CACHE_EXPIRY_MINUTES').first()
    if expiry_setting:
        try:
            expiry_minutes = int(expiry_setting.value)
        except ValueError:
            pass

    max_setting = LocalSettings.objects.filter(key='NODE_CACHE_MAX_ENTRIES').first()
    if max_setting:
        try:
            max_entries = int(max_setting.value)
        except ValueError:
            pass

    cutoff = timezone.now() - timedelta(minutes=expiry_minutes)

    mem_entry = _memory_cache.get(pubkey)
    if mem_entry:
        info, updated_at = mem_entry
        if updated_at >= cutoff:
            _memory_cache.move_to_end(pubkey)
            return info
        else:
            _memory_cache.pop(pubkey, None)

    cache = NodeCache.objects.filter(pubkey=pubkey).first()
    if cache and cache.updated_at >= cutoff:
        info = ParseDict(cache.data, ln.NodeInfo())
    else:
        try:
            # 5s gRPC timeout so a single unresponsive GetNodeInfo can't
            # block the entire [Data] loop. Fall back to stale cache (or an
            # empty NodeInfo if we have nothing) on failure.
            info = stub.GetNodeInfo(ln.NodeInfoRequest(pub_key=pubkey, include_channels=False), timeout=5)
            NodeCache.objects.update_or_create(
                pubkey=pubkey,
                defaults={'data': MessageToDict(info), 'updated_at': timezone.now()},
            )
        except Exception:
            if cache:
                info = ParseDict(cache.data, ln.NodeInfo())
            else:
                info = ln.NodeInfo()

    if len(_memory_cache) >= max_entries:
        _memory_cache.popitem(last=False)
    _memory_cache[pubkey] = (info, timezone.now())
    return info


def cache_stats():
    """Return current size and memory usage of the in-memory node cache."""
    total_bytes = 0
    for info, _ in _memory_cache.values():
        try:
            total_bytes += info.ByteSize()
        except Exception:
            # Fallback for unexpected objects
            total_bytes += 0
    return len(_memory_cache), total_bytes

