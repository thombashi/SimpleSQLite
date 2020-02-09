class NullLogger(object):
    level_name = None

    def critical(self, *args, **kwargs):  # pragma: no cover
        pass

    def debug(self, *args, **kwargs):  # pragma: no cover
        pass

    def disable(self, name):  # pragma: no cover
        pass

    def enable(self, name):  # pragma: no cover
        pass

    def error(self, *args, **kwargs):  # pragma: no cover
        pass

    def exception(self, *args, **kwargs):  # pragma: no cover
        pass

    def info(self, *args, **kwargs):  # pragma: no cover
        pass

    def log(self, level, *args, **kwargs):  # pragma: no cover
        pass

    def notice(self, *args, **kwargs):  # pragma: no cover
        pass

    def success(self, *args, **kwargs):  # pragma: no cover
        pass

    def trace(self, *args, **kwargs):  # pragma: no cover
        pass

    def warning(self, *args, **kwargs):  # pragma: no cover
        pass
