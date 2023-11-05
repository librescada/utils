"""
    This module generates credentials for the services and users defined in the configuration file `--conf_file`.
    The credentials are stored in the output directory `--output_dir` in the following format:
    - global txt credentials file for each service
    - individual txt files for each service and user
    - global environment file for all services

    Based on [FreeOpcUa generate_certificates](https://github.com/FreeOpcUa/opcua-asyncio/blob/master/examples/generate_certificates.py)

    Expected configuration file format:
    {
      "n_characters_in_password": 20,

      "services":{

        "opc_server": {
          "prefix": "UA",
          "users":{
              "ADMIN": "admin",
              "USER": "user",
              "OPERATOR": "operator",
              "DATALOGGER": "data_logger"
          }
        }

        "database": {
          "prefix": "DB",
          "users":{
              "ADMIN": "admin",
              "USER": "user",
          }
        }

      }
    }

    Example:
    python credentials_generator.py --conf_file configuration_files/credentials.hjson --output_dir .secrets
"""

from typing import Dict, List
import asyncio
from pathlib import Path
import socket
from cryptography import x509
from cryptography.hazmat.primitives.serialization import Encoding  # , load_pem_private_key
from cryptography.x509.oid import ExtendedKeyUsageOID
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from asyncua.crypto.uacrypto import load_certificate,  load_private_key

from asyncua.crypto.cert_gen import generate_private_key, generate_self_signed_app_certificate, dump_private_key_as_pem, generate_app_certificate_signing_request, sign_certificate_request


#
import secrets
import string
import hjson
import argparse
import os
from librescada_utils.logger import logger

argparser = argparse.ArgumentParser()
argparser.add_argument('-c', '--conf_file', help='Path to the configuration file',
                       required=False, default='configuration_files/credentials.hjson')
argparser.add_argument('-o', '--output_dir', help='Path to the output directory',
                       required=False, default='./certificates')
argparser.add_argument('--regenerate_CA', action=argparse.BooleanOptionalAction, default=False,
                       help='If set, the CA will be regenerated even if it already exists in the output directory')
argparser.add_argument('--no')

args = argparser.parse_args()

with open(args.conf_file) as f:
    config = hjson.load(f)


HOSTNAME: str = socket.gethostname()
CA_ID = 'librescada_CA'

# used for subject common part
NAMES: Dict[str, str] = {
    'countryName': 'ES',
    'stateOrProvinceName': 'Almeria',
    'localityName': 'Al',
    'organizationName': "LibreSCADA",
}

CLIENT_SERVER_USE = [ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH]

def generate_applicationgroup_ca(base_ca: Path, ca_id: str = 'librescada_CA') -> None:
    """
    Generates a self-signed certificate for an "id" CA and writes the private key and certificate data to specified files
    """
    key_file = base_ca / f'{ca_id}.pem'
    cert_file = base_ca / f'{ca_id}.der'

    if key_file.exists() or cert_file.exists():
        if not args.regenerate_CA:
            logger.info(f"CA for application group already exists in {base_ca} or {base_ca}")
            return
        else:
            logger.info(f"CA for application already exists, but will be regenerated (regenerate_CA flag set)")

    subject_alt_names: List[x509.GeneralName] = [x509.UniformResourceIdentifier(f"urn:{HOSTNAME}:{ca_id}"),
                                                 x509.DNSName(f"{HOSTNAME}")]

    key: RSAPrivateKey = generate_private_key()
    cert: x509.Certificate = generate_self_signed_app_certificate(key,
                                                                  "LibreSCADA CA",
                                                                  NAMES,
                                                                  subject_alt_names,
                                                                  extended=[])

    key_file.write_bytes(dump_private_key_as_pem(key))
    cert_file.write_bytes(cert.public_bytes(encoding=Encoding.DER))


async def generate_and_sign_csr(base_ca: Path, base_private: Path, base_csr: Path, base_certs: Path, id: str,
                                ca_id: str = 'librescada_CA', include_private_key=False, application_uri=None) -> None:

    subject_alt_names: List[x509.GeneralName] = \
        [
            x509.UniformResourceIdentifier(f"urn:{HOSTNAME}:{id}"),
            x509.DNSName(f"{HOSTNAME}"),
            x509.UniformResourceIdentifier(f"uri:{application_uri if application_uri else None}"),
        ]

    key: RSAPrivateKey = generate_private_key()
    csr: x509.CertificateSigningRequest = generate_app_certificate_signing_request(key,
                                                                                   f"{id}@{HOSTNAME}",
                                                                                   NAMES,
                                                                                   subject_alt_names,
                                                                                   extended=CLIENT_SERVER_USE)

    csr_file = base_csr / f'{id}.csr'
    csr_file.write_bytes(csr.public_bytes(encoding=Encoding.PEM))

    if include_private_key:
        key_file = base_private / f'{id}.pem'
        key_file.write_bytes(dump_private_key_as_pem(key))

    issuer = await load_certificate(base_ca / f'{ca_id}.der')
    key_ca = await load_private_key(base_ca / f'{ca_id}.pem')

    csr = x509.load_pem_x509_csr(csr_file.read_bytes())

    cert: x509.Certificate = sign_certificate_request(csr, issuer, key_ca, days=99999)

    # Write the signed certificate to a file
    (base_certs / f'{id}.der').write_bytes(cert.public_bytes(encoding=Encoding.DER))

    logger.info(f"Generated certificate for {id} signed by {ca_id}, valid until {cert.not_valid_after} at {(base_certs / f'{id}.der')}")


async def main():
    # Create directories for storing output files if it does not exist
    out_dir = Path( args.output_dir )

    # setup the paths for the certs, keys and csr
    base = Path(out_dir)
    base_ca: Path = base / 'ca'

    base_ca.mkdir(parents=True, exist_ok=True)

    # Generate a certification agency to sign the certificates with
    generate_applicationgroup_ca(base_ca=base_ca, ca_id=CA_ID)

    for service_key in config['services']:
        service_config = config['services'][service_key]

        if service_config.get('generate_certificates', False):
            # setup the paths for the certs, keys and csr
            base = Path(out_dir) / service_key
            base_csr: Path = base / 'csr'
            base_private: Path = base / 'private'
            base_certs: Path = base / 'certs'

            base_csr.mkdir(parents=True, exist_ok=True)
            base_private.mkdir(parents=True, exist_ok=True)
            base_certs.mkdir(parents=True, exist_ok=True)

            logger.info(f"Generating certificates for {service_key}")
            application_uri = service_config.get('application_uri', None)
            # Generate a private key and certificate signing request for the service
            await generate_and_sign_csr(base_ca, base_private, base_csr, base_certs, id=service_key, ca_id=CA_ID,
                                        include_private_key=True, application_uri=application_uri)

            # Generate a private key and certificate signing request for each user/module
            for user_data in service_config['users'].values():
                user_id = user_data['username'] if isinstance(user_data, dict) else user_data
                await generate_and_sign_csr(base_ca, base_private, base_csr, base_certs, id=user_id, ca_id=CA_ID,
                                            include_private_key=True, application_uri=application_uri)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exp:
        print(exp)