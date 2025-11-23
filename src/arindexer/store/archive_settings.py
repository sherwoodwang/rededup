from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # pyright: ignore[reportMissingImports]


# Settings key constants
SETTING_FOLLOWED_SYMLINKS = 'followed_symlinks'


class ArchiveSettings:
    """Settings manager for archive configuration.

    Provides a read-only key-value interface to access settings from .aridx/settings.toml.
    This class is agnostic to the schema and usage of settings - it simply loads the TOML
    file and provides access to the raw data structure. Consumers of this class are
    responsible for interpreting and validating the settings according to their needs.

    Example:
        settings = ArchiveSettings(archive_path)
        follow_list = settings.get(SETTING_FOLLOWED_SYMLINKS, [])
        hash_algorithm = settings.get('index.hash_algorithm', 'sha256')
    """

    def __init__(self, archive_path: Path):
        """Initialize settings from TOML file.

        Loads settings from .aridx/settings.toml if it exists. If the file does not exist,
        an empty settings dictionary is used, and all get() calls will return their defaults.

        Args:
            archive_path: Path to archive root directory
        """
        self._archive_path = archive_path
        self._settings = {}

        settings_file = archive_path / '.aridx' / 'settings.toml'
        if settings_file.exists():
            with open(settings_file, 'rb') as f:
                self._settings = tomllib.load(f)

    def get(self, key: str, default=None):
        """Get a setting value by key with optional default.

        Supports both simple keys (e.g., 'followed_symlinks') and dot notation for
        accessing nested keys (e.g., 'index.hash_algorithm' accesses
        settings['index']['hash_algorithm']). Returns the default value if the key path
        does not exist or if any intermediate value is not a dictionary.

        Args:
            key: Setting key path using dot notation for nested keys
            default: Default value to return if key not found

        Returns:
            Setting value at the specified key path, or default if not found

        Examples:
            >>> settings.get(SETTING_FOLLOWED_SYMLINKS, [])
            ['dir1', 'dir2/subdir']
            >>> settings.get('nonexistent.key', 'fallback')
            'fallback'
        """
        keys = key.split('.')
        value = self._settings

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value
