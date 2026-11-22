"""Shared Core authorization; legacy draft helpers are imported only on demand."""
from pero.access.service import AccessService, AccessError, access_for, service_for
__all__ = ['AccessService', 'AccessError', 'access_for', 'service_for']

def __getattr__(name):
    legacy = {'ActivationCodeService': 'activation_codes',
              'PrivateChatAuthenticator': 'private_auth', 'AccessAuditLogger': 'audit'}
    if name not in legacy:
        raise AttributeError(name)
    from importlib import import_module
    return getattr(import_module('pero.access.' + legacy[name]), name)
