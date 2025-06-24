from gui.models import LocalSettings

def get_local_setting(key, default, cast=str):
    setting, _ = LocalSettings.objects.get_or_create(
        key=key, defaults={'value': str(default)}
    )
    try:
        return cast(setting.value)
    except (ValueError, TypeError):
        return default
