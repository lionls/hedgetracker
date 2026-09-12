"""Runtime configuration, resolved from explicit arguments, then the environment."""

from __future__ import annotations

import os
from pathlib import Path

#: Test identity used when neither an argument nor ``SEC_IDENTITY_EMAIL`` is set.
#: The SEC requires a real, monitored contact address; always override this in
#: production.
DEFAULT_SEC_IDENTITY_EMAIL = "mark@gmail.com"

#: Default data lake root, relative to the working directory.
DEFAULT_BASE_DIR = Path("data/lake")

EMAIL_ENV_VAR = "SEC_IDENTITY_EMAIL"
BASE_DIR_ENV_VAR = "HEDGETRACKER_BASE_DIR"


def sec_identity_email(explicit: str | None = None) -> str:
    """Resolve the EDGAR identity: argument, then ``SEC_IDENTITY_EMAIL``, then default."""
    resolved = explicit or os.environ.get(EMAIL_ENV_VAR) or DEFAULT_SEC_IDENTITY_EMAIL
    if "@" not in resolved:
        raise ValueError(
            f"SEC identity {resolved!r} must contain a contact e-mail address; "
            f"set {EMAIL_ENV_VAR} to 'Name <email>' so the SEC can reach you."
        )
    return resolved


def base_dir(explicit: str | Path | None = None) -> Path:
    """Resolve the data lake root: argument, then ``HEDGETRACKER_BASE_DIR``, then default."""
    resolved = explicit or os.environ.get(BASE_DIR_ENV_VAR) or DEFAULT_BASE_DIR
    return Path(resolved).expanduser()
