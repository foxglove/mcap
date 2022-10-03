import warnings


def deprecation_notice():
    warnings.warn(
        "the `mcap.mcap0` import path is deprecated, please import from `mcap` instead",
        DeprecationWarning,
        stacklevel=3,
    )
