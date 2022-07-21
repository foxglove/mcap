import warnings


def warn_mcap0_deprecated():
    warnings.warn(
        "mcap.mcap0 module content has migrated to the parent module `mcap`,"
        " please update your import path accordingly.",
        DeprecationWarning,
    )
