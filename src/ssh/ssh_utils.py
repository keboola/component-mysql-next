import base64
import binascii
import contextlib
import warnings
from io import StringIO
from typing import Tuple

from cryptography.utils import CryptographyDeprecationWarning

with warnings.catch_warnings():
    warnings.filterwarnings('ignore', category=CryptographyDeprecationWarning)
    import paramiko
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization as crypt_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from sshtunnel import SSHTunnelForwarder

from configuration import SSHConfiguration


class SomeSSHException(Exception):
    pass


def get_private_key(input_key: str, private_key_password, is_base64=False):
    if is_base64:
        key = _get_decoded_key(input_key)
    else:
        key = input_key
    try:
        if private_key_password:
            return paramiko.RSAKey.from_private_key(StringIO(key), password=private_key_password)
        else:
            return paramiko.RSAKey.from_private_key(StringIO(key))
    except paramiko.ssh_exception.SSHException as pkey_error:
        raise SomeSSHException("Invalid private key")from pkey_error


def _get_decoded_key(input_key):
    """
        Have to satisfy both encoded and not encoded keys
    """
    b64_decoded_input_key = ""
    with contextlib.suppress(binascii.Error):
        b64_decoded_input_key = base64.b64decode(input_key, validate=True).decode('utf-8')

    is_valid_b64, message_b64 = validate_ssh_private_key(b64_decoded_input_key)
    is_valid, message = validate_ssh_private_key(input_key)
    if is_valid_b64:
        final_key = b64_decoded_input_key
    elif is_valid:
        final_key = input_key
    else:
        raise SomeSSHException("\n".join([message, message_b64]))
    return final_key


def validate_ssh_private_key(ssh_private_key: str) -> Tuple[bool, str]:
    if "\n" not in ssh_private_key:
        return False, "SSH Private key is invalid, make sure it \\n characters as new lines"
    return True, ""


def create_ssh_tunnel(config: SSHConfiguration, host: str, port: int) -> SSHTunnelForwarder:
    private_key = config.pswd_private_key
    # private_key_password = ssh.get(KEY_SSH_PRIVATE_KEY_PASSWORD)
    try:
        private_key = get_private_key(private_key, config.pswd_key_password)
    except SomeSSHException as key_exc:
        raise key_exc
    ssh_tunnel_host = config.host
    ssh_remote_address = host
    ssh_remote_port = port

    ssh_username = config.username
    return SSHTunnelForwarder(ssh_address_or_host=ssh_tunnel_host,
                              ssh_pkey=private_key,
                              ssh_username=ssh_username,
                              remote_bind_address=(ssh_remote_address, ssh_remote_port),
                              local_bind_address=(config.LOCAL_BIND_ADDRESS, config.LOCAL_BIND_PORT),
                              ssh_config_file=None,
                              allow_agent=False)


def generate_ssh_key_pair(key_size: int = 2048) -> tuple[str, str]:
    """
    Generates ssh key pair
    Args:
        key_size:

    Returns: Key pair as strings (private_key, public_key)

    """
    key_pair = rsa.generate_private_key(
        backend=default_backend(),
        public_exponent=65537,
        key_size=key_size)

    private_key = key_pair.private_bytes(
        crypt_serialization.Encoding.PEM,
        crypt_serialization.PrivateFormat.PKCS8,
        crypt_serialization.NoEncryption()).decode('utf-8')
    public_key = key_pair.public_key().public_bytes(
        crypt_serialization.Encoding.OpenSSH,
        crypt_serialization.PublicFormat.OpenSSH).decode('utf-8')
    return private_key, public_key
