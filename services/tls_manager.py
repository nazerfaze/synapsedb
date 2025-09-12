"""
SynapseDB Mutual TLS Certificate Management System
Handles certificate generation, distribution, rotation, and validation
"""

import asyncio
import os
import ssl
import logging
import time
import json
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import hashlib
import base64

from cryptography import x509
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption

logger = logging.getLogger(__name__)

@dataclass
class CertificateInfo:
    """Information about a certificate"""
    common_name: str
    serial_number: str
    issuer: str
    subject: str
    not_before: datetime
    not_after: datetime
    fingerprint: str
    key_usage: List[str]
    san_names: List[str]
    is_ca: bool = False
    
    @property
    def is_expired(self) -> bool:
        return datetime.utcnow() > self.not_after
    
    @property
    def expires_soon(self, days: int = 30) -> bool:
        return datetime.utcnow() + timedelta(days=days) > self.not_after
    
    @property
    def days_until_expiry(self) -> int:
        delta = self.not_after - datetime.utcnow()
        return max(0, delta.days)

@dataclass
class CertificateBundle:
    """A complete certificate bundle with private key"""
    certificate: bytes  # PEM encoded certificate
    private_key: bytes  # PEM encoded private key
    ca_certificate: bytes  # PEM encoded CA certificate
    certificate_chain: List[bytes]  # Full chain if applicable
    info: CertificateInfo
    
    def save_to_files(self, base_path: str, filename_prefix: str):
        """Save certificate bundle to files"""
        cert_file = f"{base_path}/{filename_prefix}.crt"
        key_file = f"{base_path}/{filename_prefix}.key"
        ca_file = f"{base_path}/{filename_prefix}-ca.crt"
        
        with open(cert_file, 'wb') as f:
            f.write(self.certificate)
        
        with open(key_file, 'wb') as f:
            f.write(self.private_key)
        
        with open(ca_file, 'wb') as f:
            f.write(self.ca_certificate)
        
        # Set secure permissions
        os.chmod(key_file, 0o600)
        os.chmod(cert_file, 0o644)
        os.chmod(ca_file, 0o644)
        
        logger.info(f"Saved certificate bundle to {cert_file}, {key_file}, {ca_file}")

class CertificateAuthority:
    """Internal Certificate Authority for the cluster"""
    
    def __init__(self, ca_key_path: str, ca_cert_path: str):
        self.ca_key_path = ca_key_path
        self.ca_cert_path = ca_cert_path
        self.ca_private_key = None
        self.ca_certificate = None
        self.serial_number = 1
    
    async def initialize(self, ca_common_name: str = "SynapseDB Cluster CA"):
        """Initialize or load the CA"""
        if os.path.exists(self.ca_key_path) and os.path.exists(self.ca_cert_path):
            await self._load_ca()
        else:
            await self._create_ca(ca_common_name)
    
    async def _create_ca(self, common_name: str):
        """Create a new Certificate Authority"""
        logger.info(f"Creating new CA: {common_name}")
        
        # Generate private key
        self.ca_private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=4096
        )
        
        # Create CA certificate
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "CA"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "SynapseDB"),
            x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Infrastructure"),
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ])
        
        now = datetime.utcnow()
        
        builder = x509.CertificateBuilder()
        builder = builder.subject_name(subject)
        builder = builder.issuer_name(issuer)
        builder = builder.public_key(self.ca_private_key.public_key())
        builder = builder.serial_number(1)
        builder = builder.not_valid_before(now)
        builder = builder.not_valid_after(now + timedelta(days=3650))  # 10 years
        
        # CA extensions
        builder = builder.add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True
        )
        
        builder = builder.add_extension(
            x509.KeyUsage(
                key_cert_sign=True,
                crl_sign=True,
                digital_signature=False,
                key_encipherment=False,
                key_agreement=False,
                data_encipherment=False,
                content_commitment=False,
                encipher_only=False,
                decipher_only=False
            ),
            critical=True
        )
        
        builder = builder.add_extension(
            x509.SubjectKeyIdentifier.from_public_key(self.ca_private_key.public_key()),
            critical=False
        )
        
        self.ca_certificate = builder.sign(self.ca_private_key, hashes.SHA256())
        
        # Save to files
        await self._save_ca()
        
        logger.info(f"Created CA certificate: {common_name}")
    
    async def _load_ca(self):
        """Load existing CA certificate and private key"""
        try:
            with open(self.ca_key_path, 'rb') as f:
                self.ca_private_key = serialization.load_pem_private_key(
                    f.read(), password=None
                )
            
            with open(self.ca_cert_path, 'rb') as f:
                self.ca_certificate = x509.load_pem_x509_certificate(f.read())
            
            # Get next serial number from certificate
            self.serial_number = int(time.time() * 1000)  # Use timestamp as serial
            
            logger.info(f"Loaded CA certificate: {self.ca_certificate.subject}")
            
        except Exception as e:
            logger.error(f"Failed to load CA: {e}")
            raise
    
    async def _save_ca(self):
        """Save CA certificate and private key to files"""
        os.makedirs(os.path.dirname(self.ca_key_path), exist_ok=True)
        
        # Save private key
        with open(self.ca_key_path, 'wb') as f:
            f.write(self.ca_private_key.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption()
            ))
        
        # Save certificate
        with open(self.ca_cert_path, 'wb') as f:
            f.write(self.ca_certificate.public_bytes(Encoding.PEM))
        
        # Set secure permissions
        os.chmod(self.ca_key_path, 0o600)
        os.chmod(self.ca_cert_path, 0o644)
    
    def generate_certificate(self, common_name: str, san_names: List[str] = None,
                           key_usage_server: bool = True, key_usage_client: bool = True,
                           validity_days: int = 365) -> CertificateBundle:
        """Generate a new certificate signed by this CA"""
        if not self.ca_private_key or not self.ca_certificate:
            raise Exception("CA not initialized")
        
        logger.info(f"Generating certificate for {common_name}")
        
        # Generate private key for the certificate
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        
        # Create certificate
        subject = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "CA"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "SynapseDB"),
            x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "Cluster"),
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ])
        
        now = datetime.utcnow()
        self.serial_number += 1
        
        builder = x509.CertificateBuilder()
        builder = builder.subject_name(subject)
        builder = builder.issuer_name(self.ca_certificate.subject)
        builder = builder.public_key(private_key.public_key())
        builder = builder.serial_number(self.serial_number)
        builder = builder.not_valid_before(now)
        builder = builder.not_valid_after(now + timedelta(days=validity_days))
        
        # Add SAN (Subject Alternative Names)
        if san_names:
            san_list = []
            for name in san_names:
                if name.replace('.', '').replace('-', '').isalnum():
                    # Looks like a hostname
                    san_list.append(x509.DNSName(name))
                else:
                    try:
                        # Try as IP address
                        san_list.append(x509.IPAddress(ipaddress.ip_address(name)))
                    except:
                        # Fallback to DNS name
                        san_list.append(x509.DNSName(name))
            
            if san_list:
                builder = builder.add_extension(
                    x509.SubjectAlternativeName(san_list),
                    critical=False
                )
        
        # Key usage
        key_usage_flags = x509.KeyUsage(
            digital_signature=True,
            key_encipherment=key_usage_server,
            key_agreement=False,
            key_cert_sign=False,
            crl_sign=False,
            data_encipherment=False,
            content_commitment=False,
            encipher_only=False,
            decipher_only=False
        )
        builder = builder.add_extension(key_usage_flags, critical=True)
        
        # Extended key usage
        extended_usage = []
        if key_usage_server:
            extended_usage.append(ExtendedKeyUsageOID.SERVER_AUTH)
        if key_usage_client:
            extended_usage.append(ExtendedKeyUsageOID.CLIENT_AUTH)
        
        if extended_usage:
            builder = builder.add_extension(
                x509.ExtendedKeyUsage(extended_usage),
                critical=True
            )
        
        # Subject Key Identifier
        builder = builder.add_extension(
            x509.SubjectKeyIdentifier.from_public_key(private_key.public_key()),
            critical=False
        )
        
        # Authority Key Identifier
        builder = builder.add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(self.ca_private_key.public_key()),
            critical=False
        )
        
        # Sign the certificate
        certificate = builder.sign(self.ca_private_key, hashes.SHA256())
        
        # Create certificate info
        fingerprint = hashlib.sha256(certificate.public_bytes(Encoding.DER)).hexdigest()
        cert_info = CertificateInfo(
            common_name=common_name,
            serial_number=str(certificate.serial_number),
            issuer=str(certificate.issuer),
            subject=str(certificate.subject),
            not_before=certificate.not_valid_before,
            not_after=certificate.not_valid_after,
            fingerprint=fingerprint,
            key_usage=[],  # Would need to parse from extensions
            san_names=san_names or [],
            is_ca=False
        )
        
        # Create bundle
        bundle = CertificateBundle(
            certificate=certificate.public_bytes(Encoding.PEM),
            private_key=private_key.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption()
            ),
            ca_certificate=self.ca_certificate.public_bytes(Encoding.PEM),
            certificate_chain=[certificate.public_bytes(Encoding.PEM)],
            info=cert_info
        )
        
        logger.info(f"Generated certificate for {common_name}, expires {cert_info.not_after}")
        return bundle
    
    def get_ca_certificate_pem(self) -> bytes:
        """Get CA certificate in PEM format"""
        return self.ca_certificate.public_bytes(Encoding.PEM)

class TLSManager:
    """Manages TLS certificates for the entire cluster"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.cluster_nodes = config.get('cluster_nodes', [])
        
        # Paths
        self.certs_dir = config.get('certs_dir', '/opt/synapsedb/certs')
        self.ca_key_path = f"{self.certs_dir}/ca/ca.key"
        self.ca_cert_path = f"{self.certs_dir}/ca/ca.crt"
        
        # Certificate Authority
        self.ca = CertificateAuthority(self.ca_key_path, self.ca_cert_path)
        
        # Certificate storage
        self.certificates: Dict[str, CertificateBundle] = {}
        
        # Settings
        self.cert_validity_days = config.get('cert_validity_days', 365)
        self.renewal_threshold_days = config.get('renewal_threshold_days', 30)
        self.auto_renewal = config.get('auto_renewal', True)
        
        # Running state
        self.running = False
    
    async def initialize(self):
        """Initialize TLS manager"""
        logger.info("Initializing TLS manager")
        
        # Create directories
        os.makedirs(f"{self.certs_dir}/ca", exist_ok=True)
        os.makedirs(f"{self.certs_dir}/nodes", exist_ok=True)
        os.makedirs(f"{self.certs_dir}/clients", exist_ok=True)
        
        # Initialize CA
        await self.ca.initialize()
        
        # Load existing certificates
        await self._load_existing_certificates()
        
        logger.info("TLS manager initialized")
    
    async def start(self):
        """Start TLS manager background tasks"""
        self.running = True
        logger.info("Starting TLS manager")
        
        tasks = [
            asyncio.create_task(self._certificate_renewal_loop()),
            asyncio.create_task(self._certificate_distribution_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"TLS manager error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop TLS manager"""
        self.running = False
        logger.info("TLS manager stopped")
    
    async def generate_node_certificate(self, node_id: str, hostname: str, 
                                       ip_addresses: List[str] = None) -> CertificateBundle:
        """Generate certificate for a cluster node"""
        logger.info(f"Generating node certificate for {node_id}")
        
        # Build SAN list
        san_names = [hostname, node_id]
        if ip_addresses:
            san_names.extend(ip_addresses)
        
        # Add localhost for local development
        san_names.extend(['localhost', '127.0.0.1'])
        
        # Remove duplicates
        san_names = list(set(san_names))
        
        bundle = self.ca.generate_certificate(
            common_name=node_id,
            san_names=san_names,
            key_usage_server=True,
            key_usage_client=True,
            validity_days=self.cert_validity_days
        )
        
        # Store certificate
        self.certificates[node_id] = bundle
        
        # Save to files
        bundle.save_to_files(f"{self.certs_dir}/nodes", node_id)
        
        logger.info(f"Generated node certificate for {node_id}")
        return bundle
    
    async def generate_client_certificate(self, client_name: str) -> CertificateBundle:
        """Generate certificate for a client application"""
        logger.info(f"Generating client certificate for {client_name}")
        
        bundle = self.ca.generate_certificate(
            common_name=client_name,
            san_names=[client_name],
            key_usage_server=False,
            key_usage_client=True,
            validity_days=self.cert_validity_days
        )
        
        # Store certificate
        self.certificates[f"client-{client_name}"] = bundle
        
        # Save to files
        bundle.save_to_files(f"{self.certs_dir}/clients", client_name)
        
        logger.info(f"Generated client certificate for {client_name}")
        return bundle
    
    async def renew_certificate(self, cert_id: str) -> Optional[CertificateBundle]:
        """Renew an existing certificate"""
        if cert_id not in self.certificates:
            logger.warning(f"Certificate {cert_id} not found for renewal")
            return None
        
        old_bundle = self.certificates[cert_id]
        logger.info(f"Renewing certificate {cert_id}")
        
        # Generate new certificate with same properties
        new_bundle = self.ca.generate_certificate(
            common_name=old_bundle.info.common_name,
            san_names=old_bundle.info.san_names,
            key_usage_server=True,
            key_usage_client=True,
            validity_days=self.cert_validity_days
        )
        
        # Update storage
        self.certificates[cert_id] = new_bundle
        
        # Save to files
        if cert_id.startswith('client-'):
            client_name = cert_id[7:]  # Remove 'client-' prefix
            new_bundle.save_to_files(f"{self.certs_dir}/clients", client_name)
        else:
            new_bundle.save_to_files(f"{self.certs_dir}/nodes", cert_id)
        
        logger.info(f"Renewed certificate {cert_id}")
        return new_bundle
    
    async def revoke_certificate(self, cert_id: str):
        """Revoke a certificate (placeholder for CRL functionality)"""
        if cert_id in self.certificates:
            logger.info(f"Revoking certificate {cert_id}")
            del self.certificates[cert_id]
            
            # In a full implementation, this would add to a Certificate Revocation List
            # For now, we just remove it from active certificates
    
    def get_certificate(self, cert_id: str) -> Optional[CertificateBundle]:
        """Get a certificate bundle by ID"""
        return self.certificates.get(cert_id)
    
    def verify_certificate_chain(self, cert_pem: bytes) -> bool:
        """Verify a certificate against our CA"""
        try:
            cert = x509.load_pem_x509_certificate(cert_pem)
            
            # Verify signature
            ca_public_key = self.ca.ca_certificate.public_key()
            ca_public_key.verify(
                cert.signature,
                cert.tbs_certificate_bytes,
                padding.PKCS1v15(),
                cert.signature_hash_algorithm
            )
            
            # Check validity period
            now = datetime.utcnow()
            if not (cert.not_valid_before <= now <= cert.not_valid_after):
                return False
            
            # Check issuer
            if cert.issuer != self.ca.ca_certificate.subject:
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Certificate verification failed: {e}")
            return False
    
    def create_ssl_context(self, cert_id: str, purpose: str = "server") -> Optional[ssl.SSLContext]:
        """Create SSL context for a certificate"""
        bundle = self.certificates.get(cert_id)
        if not bundle:
            logger.error(f"Certificate {cert_id} not found")
            return None
        
        try:
            if purpose == "server":
                context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                context.minimum_version = ssl.TLSVersion.TLSv1_2
            else:  # client
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
                context.minimum_version = ssl.TLSVersion.TLSv1_2
                context.check_hostname = False  # We'll verify manually
            
            # Load certificate and key
            context.load_cert_chain(
                certfile=None,
                keyfile=None,
                password=None
            )
            
            # Note: In practice, you'd save the cert/key to temporary files
            # and load them, or use in-memory loading if supported
            
            # Load CA certificate
            context.load_verify_locations(cadata=bundle.ca_certificate.decode())
            
            # Require client certificates for mutual TLS
            if purpose == "server":
                context.verify_mode = ssl.CERT_REQUIRED
            else:
                context.verify_mode = ssl.CERT_REQUIRED
            
            return context
            
        except Exception as e:
            logger.error(f"Failed to create SSL context for {cert_id}: {e}")
            return None
    
    async def _certificate_renewal_loop(self):
        """Background loop for automatic certificate renewal"""
        while self.running and self.auto_renewal:
            try:
                current_time = datetime.utcnow()
                
                for cert_id, bundle in list(self.certificates.items()):
                    days_until_expiry = bundle.info.days_until_expiry
                    
                    if days_until_expiry <= self.renewal_threshold_days:
                        logger.info(f"Certificate {cert_id} expires in {days_until_expiry} days, renewing")
                        await self.renew_certificate(cert_id)
                
                # Check every 6 hours
                await asyncio.sleep(6 * 3600)
                
            except Exception as e:
                logger.error(f"Error in certificate renewal loop: {e}")
                await asyncio.sleep(3600)  # Wait 1 hour on error
    
    async def _certificate_distribution_loop(self):
        """Background loop for distributing certificates to nodes"""
        while self.running:
            try:
                # In a full implementation, this would distribute certificates
                # to all cluster nodes via secure channels
                
                # For now, we just log the status
                logger.debug(f"Certificate distribution check: {len(self.certificates)} certificates active")
                
                await asyncio.sleep(3600)  # Check every hour
                
            except Exception as e:
                logger.error(f"Error in certificate distribution loop: {e}")
                await asyncio.sleep(1800)  # Wait 30 minutes on error
    
    async def _load_existing_certificates(self):
        """Load existing certificates from disk"""
        try:
            # Load node certificates
            nodes_dir = Path(f"{self.certs_dir}/nodes")
            if nodes_dir.exists():
                for cert_file in nodes_dir.glob("*.crt"):
                    node_id = cert_file.stem
                    key_file = cert_file.with_suffix('.key')
                    ca_file = cert_file.with_name(f"{node_id}-ca.crt")
                    
                    if key_file.exists() and ca_file.exists():
                        bundle = self._load_certificate_bundle(cert_file, key_file, ca_file)
                        if bundle:
                            self.certificates[node_id] = bundle
                            logger.debug(f"Loaded node certificate: {node_id}")
            
            # Load client certificates
            clients_dir = Path(f"{self.certs_dir}/clients")
            if clients_dir.exists():
                for cert_file in clients_dir.glob("*.crt"):
                    client_name = cert_file.stem
                    key_file = cert_file.with_suffix('.key')
                    ca_file = cert_file.with_name(f"{client_name}-ca.crt")
                    
                    if key_file.exists() and ca_file.exists():
                        bundle = self._load_certificate_bundle(cert_file, key_file, ca_file)
                        if bundle:
                            self.certificates[f"client-{client_name}"] = bundle
                            logger.debug(f"Loaded client certificate: {client_name}")
            
            logger.info(f"Loaded {len(self.certificates)} existing certificates")
            
        except Exception as e:
            logger.error(f"Failed to load existing certificates: {e}")
    
    def _load_certificate_bundle(self, cert_file: Path, key_file: Path, 
                                ca_file: Path) -> Optional[CertificateBundle]:
        """Load a certificate bundle from files"""
        try:
            with open(cert_file, 'rb') as f:
                cert_pem = f.read()
            
            with open(key_file, 'rb') as f:
                key_pem = f.read()
            
            with open(ca_file, 'rb') as f:
                ca_pem = f.read()
            
            # Parse certificate for info
            cert = x509.load_pem_x509_certificate(cert_pem)
            fingerprint = hashlib.sha256(cert.public_bytes(Encoding.DER)).hexdigest()
            
            cert_info = CertificateInfo(
                common_name=cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value,
                serial_number=str(cert.serial_number),
                issuer=str(cert.issuer),
                subject=str(cert.subject),
                not_before=cert.not_valid_before,
                not_after=cert.not_valid_after,
                fingerprint=fingerprint,
                key_usage=[],
                san_names=[],
                is_ca=False
            )
            
            return CertificateBundle(
                certificate=cert_pem,
                private_key=key_pem,
                ca_certificate=ca_pem,
                certificate_chain=[cert_pem],
                info=cert_info
            )
            
        except Exception as e:
            logger.error(f"Failed to load certificate bundle from {cert_file}: {e}")
            return None
    
    def get_certificate_status(self) -> Dict[str, Any]:
        """Get status of all certificates"""
        status = {
            'ca_certificate': {
                'subject': str(self.ca.ca_certificate.subject),
                'not_before': self.ca.ca_certificate.not_valid_before.isoformat(),
                'not_after': self.ca.ca_certificate.not_valid_after.isoformat(),
                'serial_number': str(self.ca.ca_certificate.serial_number)
            },
            'certificates': {}
        }
        
        for cert_id, bundle in self.certificates.items():
            status['certificates'][cert_id] = {
                'common_name': bundle.info.common_name,
                'not_before': bundle.info.not_before.isoformat(),
                'not_after': bundle.info.not_after.isoformat(),
                'days_until_expiry': bundle.info.days_until_expiry,
                'is_expired': bundle.info.is_expired,
                'expires_soon': bundle.info.expires_soon,
                'fingerprint': bundle.info.fingerprint,
                'san_names': bundle.info.san_names
            }
        
        return status
    
    async def generate_cluster_certificates(self):
        """Generate certificates for all nodes in the cluster"""
        logger.info("Generating certificates for all cluster nodes")
        
        for node_config in self.cluster_nodes:
            node_id = node_config['id']
            hostname = node_config['host']
            ip_addresses = node_config.get('ip_addresses', [])
            
            if node_id not in self.certificates:
                await self.generate_node_certificate(node_id, hostname, ip_addresses)
            else:
                logger.debug(f"Certificate for node {node_id} already exists")
        
        logger.info(f"Certificate generation complete: {len(self.certificates)} certificates")