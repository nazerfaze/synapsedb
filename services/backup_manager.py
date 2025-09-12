"""
SynapseDB Distributed Point-in-Time Backup System
Coordinates consistent backups across all cluster nodes with incremental capabilities
"""

import asyncio
import os
import logging
import time
import json
import gzip
import hashlib
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
from datetime import datetime, timedelta
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import asyncpg

logger = logging.getLogger(__name__)

class BackupType(Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    WAL = "wal"

class BackupStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"

class StorageType(Enum):
    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"
    AZURE = "azure"

@dataclass
class BackupMetadata:
    """Metadata for a backup"""
    backup_id: str
    backup_type: BackupType
    status: BackupStatus
    coordinator_node: str
    participant_nodes: List[str]
    start_time: datetime
    end_time: Optional[datetime] = None
    size_bytes: int = 0
    compressed_size_bytes: int = 0
    lsn: Optional[str] = None  # PostgreSQL Log Sequence Number
    parent_backup_id: Optional[str] = None  # For incremental backups
    storage_location: str = ""
    checksum: str = ""
    retention_until: datetime = None
    files: List[Dict[str, Any]] = field(default_factory=list)
    
    @property
    def duration_seconds(self) -> float:
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def compression_ratio(self) -> float:
        if self.size_bytes > 0:
            return self.compressed_size_bytes / self.size_bytes
        return 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['start_time'] = self.start_time.isoformat()
        data['end_time'] = self.end_time.isoformat() if self.end_time else None
        data['retention_until'] = self.retention_until.isoformat() if self.retention_until else None
        data['backup_type'] = self.backup_type.value
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BackupMetadata':
        data = data.copy()
        data['start_time'] = datetime.fromisoformat(data['start_time'])
        data['end_time'] = datetime.fromisoformat(data['end_time']) if data['end_time'] else None
        data['retention_until'] = datetime.fromisoformat(data['retention_until']) if data['retention_until'] else None
        data['backup_type'] = BackupType(data['backup_type'])
        data['status'] = BackupStatus(data['status'])
        return cls(**data)

class StorageBackend:
    """Abstract base for storage backends"""
    
    async def upload_file(self, local_path: str, remote_path: str) -> str:
        """Upload file and return remote URL"""
        raise NotImplementedError
    
    async def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download file from remote storage"""
        raise NotImplementedError
    
    async def list_files(self, prefix: str) -> List[str]:
        """List files with given prefix"""
        raise NotImplementedError
    
    async def delete_file(self, remote_path: str) -> bool:
        """Delete file from remote storage"""
        raise NotImplementedError
    
    def get_url(self, remote_path: str) -> str:
        """Get URL for remote path"""
        raise NotImplementedError

class LocalStorageBackend(StorageBackend):
    """Local filesystem storage backend"""
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    async def upload_file(self, local_path: str, remote_path: str) -> str:
        """Copy file to backup location"""
        try:
            full_remote_path = self.base_path / remote_path
            full_remote_path.parent.mkdir(parents=True, exist_ok=True)
            
            import shutil
            shutil.copy2(local_path, full_remote_path)
            
            return str(full_remote_path)
        except Exception as e:
            logger.error(f"Failed to upload file to local storage: {e}")
            raise
    
    async def download_file(self, remote_path: str, local_path: str) -> bool:
        """Copy file from backup location"""
        try:
            full_remote_path = self.base_path / remote_path
            if not full_remote_path.exists():
                return False
            
            import shutil
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            shutil.copy2(full_remote_path, local_path)
            
            return True
        except Exception as e:
            logger.error(f"Failed to download file from local storage: {e}")
            return False
    
    async def list_files(self, prefix: str) -> List[str]:
        """List files with given prefix"""
        try:
            prefix_path = self.base_path / prefix
            if not prefix_path.exists():
                return []
            
            files = []
            for file_path in prefix_path.rglob('*'):
                if file_path.is_file():
                    relative_path = file_path.relative_to(self.base_path)
                    files.append(str(relative_path))
            
            return files
        except Exception as e:
            logger.error(f"Failed to list files from local storage: {e}")
            return []
    
    async def delete_file(self, remote_path: str) -> bool:
        """Delete file from backup location"""
        try:
            full_remote_path = self.base_path / remote_path
            if full_remote_path.exists():
                full_remote_path.unlink()
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to delete file from local storage: {e}")
            return False
    
    def get_url(self, remote_path: str) -> str:
        """Get file:// URL for local path"""
        full_path = self.base_path / remote_path
        return f"file://{full_path}"

class S3StorageBackend(StorageBackend):
    """Amazon S3 storage backend"""
    
    def __init__(self, bucket: str, prefix: str = "", 
                 aws_access_key_id: str = None, aws_secret_access_key: str = None,
                 region: str = "us-east-1"):
        self.bucket = bucket
        self.prefix = prefix.rstrip('/')
        self.region = region
        
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )
        self.s3 = session.client('s3')
    
    async def upload_file(self, local_path: str, remote_path: str) -> str:
        """Upload file to S3"""
        try:
            s3_key = f"{self.prefix}/{remote_path}" if self.prefix else remote_path
            
            # Upload with server-side encryption
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3.upload_file(
                    local_path, self.bucket, s3_key,
                    ExtraArgs={'ServerSideEncryption': 'AES256'}
                )
            )
            
            return f"s3://{self.bucket}/{s3_key}"
        except Exception as e:
            logger.error(f"Failed to upload file to S3: {e}")
            raise
    
    async def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download file from S3"""
        try:
            s3_key = f"{self.prefix}/{remote_path}" if self.prefix else remote_path
            
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3.download_file(self.bucket, s3_key, local_path)
            )
            
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.warning(f"File not found in S3: {remote_path}")
            else:
                logger.error(f"Failed to download file from S3: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to download file from S3: {e}")
            return False
    
    async def list_files(self, prefix: str) -> List[str]:
        """List files in S3 with given prefix"""
        try:
            s3_prefix = f"{self.prefix}/{prefix}" if self.prefix else prefix
            
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3.list_objects_v2(Bucket=self.bucket, Prefix=s3_prefix)
            )
            
            files = []
            for obj in response.get('Contents', []):
                key = obj['Key']
                if self.prefix:
                    key = key[len(self.prefix)+1:]  # Remove prefix
                files.append(key)
            
            return files
        except Exception as e:
            logger.error(f"Failed to list files from S3: {e}")
            return []
    
    async def delete_file(self, remote_path: str) -> bool:
        """Delete file from S3"""
        try:
            s3_key = f"{self.prefix}/{remote_path}" if self.prefix else remote_path
            
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3.delete_object(Bucket=self.bucket, Key=s3_key)
            )
            
            return True
        except Exception as e:
            logger.error(f"Failed to delete file from S3: {e}")
            return False
    
    def get_url(self, remote_path: str) -> str:
        """Get S3 URL"""
        s3_key = f"{self.prefix}/{remote_path}" if self.prefix else remote_path
        return f"s3://{self.bucket}/{s3_key}"

class BackupCoordinator:
    """Coordinates distributed backups across cluster"""
    
    def __init__(self, node_id: str, cluster_nodes: List[Dict], storage_config: Dict,
                 db_config: Dict):
        self.node_id = node_id
        self.cluster_nodes = {n['id']: n for n in cluster_nodes}
        self.storage_config = storage_config
        self.db_config = db_config
        
        # Database connection
        self.db_pool: Optional[asyncpg.Pool] = None
        
        # Storage backend
        self.storage = self._create_storage_backend()
        
        # Backup tracking
        self.active_backups: Dict[str, BackupMetadata] = {}
        self.backup_history: List[BackupMetadata] = []
        
        # Settings
        self.backup_retention_days = storage_config.get('retention_days', 30)
        self.max_parallel_backups = storage_config.get('max_parallel_backups', 3)
        self.wal_archive_enabled = storage_config.get('wal_archive', True)
        
        # WAL archiving
        self.wal_archive_queue = asyncio.Queue()
        self.wal_sequence_number = 0
        
        # Running state
        self.running = False
    
    def _create_storage_backend(self) -> StorageBackend:
        """Create appropriate storage backend"""
        storage_type = StorageType(self.storage_config.get('type', 'local'))
        
        if storage_type == StorageType.LOCAL:
            return LocalStorageBackend(self.storage_config.get('path', '/opt/synapsedb/backups'))
        elif storage_type == StorageType.S3:
            return S3StorageBackend(
                bucket=self.storage_config['bucket'],
                prefix=self.storage_config.get('prefix', 'synapsedb-backups'),
                aws_access_key_id=self.storage_config.get('aws_access_key_id'),
                aws_secret_access_key=self.storage_config.get('aws_secret_access_key'),
                region=self.storage_config.get('region', 'us-east-1')
            )
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
    
    async def initialize(self):
        """Initialize backup manager"""
        logger.info(f"Initializing backup manager for node {self.node_id}")
        
        self.db_pool = await asyncpg.create_pool(**self.db_config, min_size=2, max_size=8)
        
        # Create backup tracking tables
        await self._create_tables()
        
        # Load backup history
        await self._load_backup_history()
        
        logger.info("Backup manager initialized")
    
    async def start(self):
        """Start backup manager"""
        self.running = True
        logger.info("Starting distributed backup manager")
        
        tasks = [
            asyncio.create_task(self._backup_scheduler_loop()),
            asyncio.create_task(self._wal_archiver_loop()),
            asyncio.create_task(self._cleanup_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Backup manager error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop backup manager"""
        self.running = False
        logger.info("Stopping backup manager")
        
        if self.db_pool:
            await self.db_pool.close()
    
    async def create_backup(self, backup_type: BackupType = BackupType.FULL,
                           participant_nodes: List[str] = None) -> str:
        """Create a new distributed backup"""
        try:
            import uuid
            backup_id = str(uuid.uuid4())
            
            if participant_nodes is None:
                participant_nodes = list(self.cluster_nodes.keys())
            
            # Create backup metadata
            backup_metadata = BackupMetadata(
                backup_id=backup_id,
                backup_type=backup_type,
                status=BackupStatus.PENDING,
                coordinator_node=self.node_id,
                participant_nodes=participant_nodes,
                start_time=datetime.utcnow(),
                retention_until=datetime.utcnow() + timedelta(days=self.backup_retention_days)
            )
            
            self.active_backups[backup_id] = backup_metadata
            await self._persist_backup_metadata(backup_metadata)
            
            logger.info(f"Created backup {backup_id} with {len(participant_nodes)} participants")
            
            # Start backup process
            asyncio.create_task(self._execute_backup(backup_id))
            
            return backup_id
            
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            raise
    
    async def _execute_backup(self, backup_id: str):
        """Execute the actual backup process"""
        backup_metadata = self.active_backups[backup_id]
        
        try:
            logger.info(f"Starting backup execution: {backup_id}")
            backup_metadata.status = BackupStatus.RUNNING
            await self._persist_backup_metadata(backup_metadata)
            
            # Coordinate backup across all nodes
            if backup_metadata.backup_type == BackupType.FULL:
                await self._execute_full_backup(backup_metadata)
            elif backup_metadata.backup_type == BackupType.INCREMENTAL:
                await self._execute_incremental_backup(backup_metadata)
            else:
                raise ValueError(f"Unsupported backup type: {backup_metadata.backup_type}")
            
            # Finalize backup
            backup_metadata.end_time = datetime.utcnow()
            backup_metadata.status = BackupStatus.COMPLETED
            
            logger.info(f"Backup {backup_id} completed in {backup_metadata.duration_seconds:.1f}s, "
                       f"size: {backup_metadata.size_bytes // 1024 // 1024}MB")
            
        except Exception as e:
            logger.error(f"Backup {backup_id} failed: {e}")
            backup_metadata.status = BackupStatus.FAILED
            backup_metadata.end_time = datetime.utcnow()
        
        finally:
            await self._persist_backup_metadata(backup_metadata)
            if backup_id in self.active_backups:
                self.backup_history.append(self.active_backups[backup_id])
                del self.active_backups[backup_id]
    
    async def _execute_full_backup(self, backup_metadata: BackupMetadata):
        """Execute a full backup"""
        logger.info(f"Executing full backup {backup_metadata.backup_id}")
        
        # Get consistent LSN across all nodes
        lsn = await self._get_consistent_lsn(backup_metadata.participant_nodes)
        backup_metadata.lsn = lsn
        
        # Start backup on all participant nodes
        backup_tasks = []
        for node_id in backup_metadata.participant_nodes:
            task = asyncio.create_task(
                self._backup_node_data(backup_metadata.backup_id, node_id, BackupType.FULL, lsn)
            )
            backup_tasks.append(task)
        
        # Wait for all node backups to complete
        results = await asyncio.gather(*backup_tasks, return_exceptions=True)
        
        # Process results
        total_size = 0
        total_compressed_size = 0
        
        for i, result in enumerate(results):
            node_id = backup_metadata.participant_nodes[i]
            
            if isinstance(result, Exception):
                logger.error(f"Backup failed on node {node_id}: {result}")
                raise result
            
            file_info, size, compressed_size = result
            backup_metadata.files.extend(file_info)
            total_size += size
            total_compressed_size += compressed_size
        
        backup_metadata.size_bytes = total_size
        backup_metadata.compressed_size_bytes = total_compressed_size
        
        # Create backup manifest
        await self._create_backup_manifest(backup_metadata)
    
    async def _execute_incremental_backup(self, backup_metadata: BackupMetadata):
        """Execute an incremental backup"""
        logger.info(f"Executing incremental backup {backup_metadata.backup_id}")
        
        # Find the most recent full backup
        parent_backup = self._find_latest_full_backup()
        if not parent_backup:
            logger.warning("No full backup found, creating full backup instead")
            backup_metadata.backup_type = BackupType.FULL
            await self._execute_full_backup(backup_metadata)
            return
        
        backup_metadata.parent_backup_id = parent_backup.backup_id
        
        # Get WAL files since last backup
        start_lsn = parent_backup.lsn
        end_lsn = await self._get_consistent_lsn(backup_metadata.participant_nodes)
        backup_metadata.lsn = end_lsn
        
        # Backup changed data and WAL files
        backup_tasks = []
        for node_id in backup_metadata.participant_nodes:
            task = asyncio.create_task(
                self._backup_incremental_data(backup_metadata.backup_id, node_id, start_lsn, end_lsn)
            )
            backup_tasks.append(task)
        
        # Wait for completion
        results = await asyncio.gather(*backup_tasks, return_exceptions=True)
        
        # Process results (similar to full backup)
        total_size = 0
        total_compressed_size = 0
        
        for i, result in enumerate(results):
            node_id = backup_metadata.participant_nodes[i]
            
            if isinstance(result, Exception):
                logger.error(f"Incremental backup failed on node {node_id}: {result}")
                raise result
            
            file_info, size, compressed_size = result
            backup_metadata.files.extend(file_info)
            total_size += size
            total_compressed_size += compressed_size
        
        backup_metadata.size_bytes = total_size
        backup_metadata.compressed_size_bytes = total_compressed_size
        
        # Create backup manifest
        await self._create_backup_manifest(backup_metadata)
    
    async def _get_consistent_lsn(self, participant_nodes: List[str]) -> str:
        """Get a consistent LSN across all nodes"""
        try:
            # For leader node, get current LSN
            async with self.db_pool.acquire() as conn:
                lsn = await conn.fetchval("SELECT pg_current_wal_lsn()")
                
                # Wait for replicas to catch up to this LSN
                for node_id in participant_nodes:
                    if node_id != self.node_id:
                        await self._wait_for_lsn_on_node(node_id, lsn)
                
                return str(lsn)
                
        except Exception as e:
            logger.error(f"Failed to get consistent LSN: {e}")
            raise
    
    async def _wait_for_lsn_on_node(self, node_id: str, target_lsn: str, timeout: float = 30.0):
        """Wait for a node to catch up to target LSN"""
        node_info = self.cluster_nodes.get(node_id)
        if not node_info:
            raise ValueError(f"Unknown node: {node_id}")
        
        node_db_config = self.db_config.copy()
        node_db_config['host'] = node_info['host']
        node_db_config['port'] = node_info.get('port', 5432)
        
        start_time = time.time()
        
        try:
            while time.time() - start_time < timeout:
                conn = await asyncpg.connect(**node_db_config)
                try:
                    current_lsn = await conn.fetchval("SELECT pg_last_wal_replay_lsn()")
                    if current_lsn and str(current_lsn) >= target_lsn:
                        logger.debug(f"Node {node_id} caught up to LSN {target_lsn}")
                        return
                finally:
                    await conn.close()
                
                await asyncio.sleep(0.1)  # Wait 100ms
            
            raise TimeoutError(f"Node {node_id} did not catch up to LSN {target_lsn} within {timeout}s")
            
        except Exception as e:
            logger.error(f"Failed to wait for LSN on node {node_id}: {e}")
            raise
    
    async def _backup_node_data(self, backup_id: str, node_id: str, backup_type: BackupType,
                               lsn: str) -> Tuple[List[Dict], int, int]:
        """Backup data from a specific node"""
        logger.info(f"Starting backup of node {node_id} for backup {backup_id}")
        
        # Create backup directory
        backup_dir = f"/tmp/synapsedb_backup_{backup_id}_{node_id}"
        os.makedirs(backup_dir, exist_ok=True)
        
        try:
            # Use pg_basebackup for full backup
            if backup_type == BackupType.FULL:
                cmd = [
                    "pg_basebackup",
                    "-h", self.cluster_nodes[node_id]['host'],
                    "-p", str(self.cluster_nodes[node_id].get('port', 5432)),
                    "-U", self.db_config['user'],
                    "-D", backup_dir,
                    "-Ft",  # tar format
                    "-z",   # gzip compression
                    "-P",   # show progress
                    "-v",   # verbose
                    "-W",   # force password prompt
                ]
                
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    env={**os.environ, 'PGPASSWORD': self.db_config['password']},
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                stdout, stderr = await process.communicate()
                
                if process.returncode != 0:
                    raise Exception(f"pg_basebackup failed: {stderr.decode()}")
            
            # Calculate sizes and create file list
            files = []
            total_size = 0
            total_compressed_size = 0
            
            for root, dirs, filenames in os.walk(backup_dir):
                for filename in filenames:
                    file_path = os.path.join(root, filename)
                    file_stat = os.stat(file_path)
                    
                    # Calculate checksum
                    with open(file_path, 'rb') as f:
                        file_checksum = hashlib.sha256(f.read()).hexdigest()
                    
                    relative_path = os.path.relpath(file_path, backup_dir)
                    remote_path = f"{backup_id}/{node_id}/{relative_path}"
                    
                    # Upload to storage
                    remote_url = await self.storage.upload_file(file_path, remote_path)
                    
                    files.append({
                        'path': relative_path,
                        'remote_path': remote_path,
                        'remote_url': remote_url,
                        'size': file_stat.st_size,
                        'checksum': file_checksum,
                        'node_id': node_id
                    })
                    
                    total_size += file_stat.st_size
                    
                    # For compressed files, use actual size as compressed size
                    if filename.endswith('.gz') or filename.endswith('.tar.gz'):
                        total_compressed_size += file_stat.st_size
                    else:
                        total_compressed_size += file_stat.st_size
            
            logger.info(f"Completed backup of node {node_id}: {len(files)} files, "
                       f"{total_size // 1024 // 1024}MB")
            
            return files, total_size, total_compressed_size
            
        finally:
            # Cleanup temporary directory
            import shutil
            if os.path.exists(backup_dir):
                shutil.rmtree(backup_dir)
    
    async def _backup_incremental_data(self, backup_id: str, node_id: str, 
                                     start_lsn: str, end_lsn: str) -> Tuple[List[Dict], int, int]:
        """Backup incremental data (WAL files) from a node"""
        logger.info(f"Starting incremental backup of node {node_id} from {start_lsn} to {end_lsn}")
        
        # This is a simplified implementation
        # In practice, you'd need to:
        # 1. Identify changed blocks using pg_waldump
        # 2. Extract only changed data pages
        # 3. Archive relevant WAL files
        
        # For now, we'll just archive WAL files
        files = []
        total_size = 0
        total_compressed_size = 0
        
        try:
            # Get WAL file list
            node_info = self.cluster_nodes[node_id]
            node_db_config = self.db_config.copy()
            node_db_config['host'] = node_info['host']
            node_db_config['port'] = node_info.get('port', 5432)
            
            conn = await asyncpg.connect(**node_db_config)
            try:
                # Get WAL files in range
                wal_files = await conn.fetch("""
                    SELECT name, size FROM pg_ls_waldir() 
                    WHERE name ~ '^[0-9A-F]{24}$'
                    ORDER BY name
                """)
                
                for wal_file in wal_files:
                    # Archive WAL file (simplified)
                    wal_name = wal_file['name']
                    wal_size = wal_file['size']
                    
                    remote_path = f"{backup_id}/{node_id}/wal/{wal_name}"
                    
                    files.append({
                        'path': f"wal/{wal_name}",
                        'remote_path': remote_path,
                        'remote_url': self.storage.get_url(remote_path),
                        'size': wal_size,
                        'checksum': '',  # Would calculate in real implementation
                        'node_id': node_id
                    })
                    
                    total_size += wal_size
                    total_compressed_size += wal_size  # Assume no compression for WAL
            finally:
                await conn.close()
        
        except Exception as e:
            logger.error(f"Incremental backup failed for node {node_id}: {e}")
            raise
        
        return files, total_size, total_compressed_size
    
    async def _create_backup_manifest(self, backup_metadata: BackupMetadata):
        """Create and upload backup manifest"""
        manifest = {
            'backup_metadata': backup_metadata.to_dict(),
            'files': backup_metadata.files,
            'created_at': datetime.utcnow().isoformat(),
            'version': '1.0'
        }
        
        # Calculate manifest checksum
        manifest_json = json.dumps(manifest, sort_keys=True)
        backup_metadata.checksum = hashlib.sha256(manifest_json.encode()).hexdigest()
        
        # Update manifest with checksum
        manifest['backup_metadata']['checksum'] = backup_metadata.checksum
        
        # Save manifest locally and upload
        manifest_path = f"/tmp/backup_manifest_{backup_metadata.backup_id}.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        # Upload manifest
        remote_manifest_path = f"{backup_metadata.backup_id}/manifest.json"
        backup_metadata.storage_location = await self.storage.upload_file(
            manifest_path, remote_manifest_path
        )
        
        # Cleanup local manifest
        os.unlink(manifest_path)
        
        logger.info(f"Created backup manifest for {backup_metadata.backup_id}")
    
    def _find_latest_full_backup(self) -> Optional[BackupMetadata]:
        """Find the most recent successful full backup"""
        full_backups = [
            backup for backup in self.backup_history
            if backup.backup_type == BackupType.FULL and backup.status == BackupStatus.COMPLETED
        ]
        
        if not full_backups:
            return None
        
        return max(full_backups, key=lambda b: b.start_time)
    
    async def restore_backup(self, backup_id: str, target_time: datetime = None,
                           target_nodes: List[str] = None) -> bool:
        """Restore from a backup"""
        logger.info(f"Starting restore from backup {backup_id}")
        
        try:
            # Load backup metadata
            backup_metadata = await self._load_backup_metadata(backup_id)
            if not backup_metadata:
                raise ValueError(f"Backup {backup_id} not found")
            
            if target_nodes is None:
                target_nodes = backup_metadata.participant_nodes
            
            # Download backup manifest
            manifest_path = f"/tmp/restore_manifest_{backup_id}.json"
            remote_manifest_path = f"{backup_id}/manifest.json"
            
            if not await self.storage.download_file(remote_manifest_path, manifest_path):
                raise Exception(f"Failed to download backup manifest for {backup_id}")
            
            # Load manifest
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
            
            # Verify checksum
            manifest_copy = manifest.copy()
            del manifest_copy['backup_metadata']['checksum']
            expected_checksum = hashlib.sha256(
                json.dumps(manifest_copy, sort_keys=True).encode()
            ).hexdigest()
            
            if manifest['backup_metadata']['checksum'] != expected_checksum:
                raise Exception("Backup manifest checksum verification failed")
            
            # Restore each node
            restore_tasks = []
            for node_id in target_nodes:
                task = asyncio.create_task(
                    self._restore_node_data(backup_id, node_id, manifest, target_time)
                )
                restore_tasks.append(task)
            
            # Wait for all restores to complete
            results = await asyncio.gather(*restore_tasks, return_exceptions=True)
            
            # Check results
            for i, result in enumerate(results):
                node_id = target_nodes[i]
                if isinstance(result, Exception):
                    logger.error(f"Restore failed on node {node_id}: {result}")
                    raise result
            
            logger.info(f"Successfully restored backup {backup_id} to {len(target_nodes)} nodes")
            return True
            
        except Exception as e:
            logger.error(f"Backup restore failed: {e}")
            return False
        finally:
            # Cleanup
            if os.path.exists(manifest_path):
                os.unlink(manifest_path)
    
    async def _restore_node_data(self, backup_id: str, node_id: str, manifest: Dict,
                               target_time: datetime = None):
        """Restore data to a specific node"""
        logger.info(f"Restoring node {node_id} from backup {backup_id}")
        
        # Create restore directory
        restore_dir = f"/tmp/synapsedb_restore_{backup_id}_{node_id}"
        os.makedirs(restore_dir, exist_ok=True)
        
        try:
            # Download all files for this node
            node_files = [f for f in manifest['files'] if f['node_id'] == node_id]
            
            for file_info in node_files:
                local_file_path = os.path.join(restore_dir, file_info['path'])
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                
                if not await self.storage.download_file(file_info['remote_path'], local_file_path):
                    raise Exception(f"Failed to download {file_info['remote_path']}")
                
                # Verify checksum
                with open(local_file_path, 'rb') as f:
                    actual_checksum = hashlib.sha256(f.read()).hexdigest()
                
                if actual_checksum != file_info['checksum']:
                    raise Exception(f"Checksum mismatch for {file_info['path']}")
            
            # Restore database (this would involve stopping PostgreSQL, 
            # replacing data directory, and restarting)
            logger.warning(f"Database restore for node {node_id} requires manual intervention")
            
        finally:
            # Cleanup restore directory
            import shutil
            if os.path.exists(restore_dir):
                shutil.rmtree(restore_dir)
    
    async def _backup_scheduler_loop(self):
        """Schedule automatic backups"""
        while self.running:
            try:
                # Check if it's time for a scheduled backup
                # This is a simplified implementation
                # In practice, you'd have configurable backup schedules
                
                current_time = datetime.utcnow()
                
                # Check for daily backup
                last_backup = self._find_latest_full_backup()
                if not last_backup or (current_time - last_backup.start_time).days >= 1:
                    logger.info("Creating scheduled full backup")
                    await self.create_backup(BackupType.FULL)
                
                # Sleep for 1 hour
                await asyncio.sleep(3600)
                
            except Exception as e:
                logger.error(f"Error in backup scheduler: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    async def _wal_archiver_loop(self):
        """Archive WAL files continuously"""
        while self.running:
            try:
                if not self.wal_archive_enabled:
                    await asyncio.sleep(60)
                    continue
                
                # Archive WAL files (simplified implementation)
                await self._archive_wal_files()
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in WAL archiver: {e}")
                await asyncio.sleep(60)
    
    async def _archive_wal_files(self):
        """Archive completed WAL files"""
        try:
            async with self.db_pool.acquire() as conn:
                # Get completed WAL files
                wal_files = await conn.fetch("""
                    SELECT name, size, modification 
                    FROM pg_ls_waldir() 
                    WHERE name ~ '^[0-9A-F]{24}$'
                    AND modification < NOW() - INTERVAL '1 minute'
                    ORDER BY name
                """)
                
                for wal_file in wal_files[:10]:  # Process max 10 at a time
                    wal_name = wal_file['name']
                    
                    # Check if already archived
                    archived = await conn.fetchval("""
                        SELECT 1 FROM synapsedb_backups.wal_archive 
                        WHERE wal_name = $1 AND node_id = $2
                    """, wal_name, self.node_id)
                    
                    if not archived:
                        await self._archive_single_wal(wal_name)
                        
        except Exception as e:
            logger.error(f"Error archiving WAL files: {e}")
    
    async def _archive_single_wal(self, wal_name: str):
        """Archive a single WAL file"""
        try:
            # Copy WAL file to temp location
            wal_source = f"{self.db_config.get('data_directory', '/var/lib/postgresql/data')}/pg_wal/{wal_name}"
            wal_temp = f"/tmp/wal_{wal_name}"
            
            import shutil
            shutil.copy2(wal_source, wal_temp)
            
            # Compress
            with open(wal_temp, 'rb') as f_in:
                with gzip.open(f"{wal_temp}.gz", 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Upload
            remote_path = f"wal/{self.node_id}/{wal_name}.gz"
            remote_url = await self.storage.upload_file(f"{wal_temp}.gz", remote_path)
            
            # Record in database
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synapsedb_backups.wal_archive 
                    (wal_name, node_id, remote_path, remote_url, archived_at)
                    VALUES ($1, $2, $3, $4, NOW())
                """, wal_name, self.node_id, remote_path, remote_url)
            
            logger.debug(f"Archived WAL file {wal_name}")
            
            # Cleanup
            os.unlink(wal_temp)
            os.unlink(f"{wal_temp}.gz")
            
        except Exception as e:
            logger.error(f"Failed to archive WAL file {wal_name}: {e}")
    
    async def _cleanup_loop(self):
        """Clean up old backups based on retention policy"""
        while self.running:
            try:
                await self._cleanup_expired_backups()
                await asyncio.sleep(86400)  # Daily cleanup
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(3600)
    
    async def _cleanup_expired_backups(self):
        """Remove expired backups"""
        current_time = datetime.utcnow()
        
        for backup in list(self.backup_history):
            if backup.retention_until and current_time > backup.retention_until:
                logger.info(f"Cleaning up expired backup {backup.backup_id}")
                
                # Delete files from storage
                for file_info in backup.files:
                    await self.storage.delete_file(file_info['remote_path'])
                
                # Delete manifest
                await self.storage.delete_file(f"{backup.backup_id}/manifest.json")
                
                # Mark as expired
                backup.status = BackupStatus.EXPIRED
                await self._persist_backup_metadata(backup)
                
                self.backup_history.remove(backup)
    
    async def _create_tables(self):
        """Create backup tracking tables"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                CREATE SCHEMA IF NOT EXISTS synapsedb_backups;
                
                CREATE TABLE IF NOT EXISTS synapsedb_backups.backup_metadata (
                    backup_id VARCHAR(64) PRIMARY KEY,
                    backup_type VARCHAR(32) NOT NULL,
                    status VARCHAR(32) NOT NULL,
                    coordinator_node VARCHAR(64) NOT NULL,
                    participant_nodes TEXT[] NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    size_bytes BIGINT DEFAULT 0,
                    compressed_size_bytes BIGINT DEFAULT 0,
                    lsn VARCHAR(32),
                    parent_backup_id VARCHAR(64),
                    storage_location TEXT,
                    checksum VARCHAR(64),
                    retention_until TIMESTAMP,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE TABLE IF NOT EXISTS synapsedb_backups.wal_archive (
                    wal_name VARCHAR(64) NOT NULL,
                    node_id VARCHAR(64) NOT NULL,
                    remote_path TEXT NOT NULL,
                    remote_url TEXT NOT NULL,
                    archived_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (wal_name, node_id)
                );
                
                CREATE INDEX IF NOT EXISTS idx_backup_metadata_coordinator_status
                ON synapsedb_backups.backup_metadata (coordinator_node, status);
                
                CREATE INDEX IF NOT EXISTS idx_backup_metadata_start_time
                ON synapsedb_backups.backup_metadata (start_time);
                
                CREATE INDEX IF NOT EXISTS idx_wal_archive_node_archived
                ON synapsedb_backups.wal_archive (node_id, archived_at);
            """)
    
    async def _persist_backup_metadata(self, backup_metadata: BackupMetadata):
        """Persist backup metadata to database"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synapsedb_backups.backup_metadata
                    (backup_id, backup_type, status, coordinator_node, participant_nodes,
                     start_time, end_time, size_bytes, compressed_size_bytes, lsn,
                     parent_backup_id, storage_location, checksum, retention_until, metadata, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW())
                    ON CONFLICT (backup_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        end_time = EXCLUDED.end_time,
                        size_bytes = EXCLUDED.size_bytes,
                        compressed_size_bytes = EXCLUDED.compressed_size_bytes,
                        lsn = EXCLUDED.lsn,
                        storage_location = EXCLUDED.storage_location,
                        checksum = EXCLUDED.checksum,
                        metadata = EXCLUDED.metadata,
                        updated_at = NOW()
                """, backup_metadata.backup_id, backup_metadata.backup_type.value,
                    backup_metadata.status.value, backup_metadata.coordinator_node,
                    backup_metadata.participant_nodes, backup_metadata.start_time,
                    backup_metadata.end_time, backup_metadata.size_bytes,
                    backup_metadata.compressed_size_bytes, backup_metadata.lsn,
                    backup_metadata.parent_backup_id, backup_metadata.storage_location,
                    backup_metadata.checksum, backup_metadata.retention_until,
                    json.dumps(backup_metadata.to_dict()))
        except Exception as e:
            logger.error(f"Failed to persist backup metadata {backup_metadata.backup_id}: {e}")
    
    async def _load_backup_metadata(self, backup_id: str) -> Optional[BackupMetadata]:
        """Load backup metadata from database"""
        try:
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT * FROM synapsedb_backups.backup_metadata
                    WHERE backup_id = $1
                """, backup_id)
                
                if row:
                    metadata_dict = json.loads(row['metadata'])
                    return BackupMetadata.from_dict(metadata_dict)
                
                return None
                
        except Exception as e:
            logger.error(f"Failed to load backup metadata {backup_id}: {e}")
            return None
    
    async def _load_backup_history(self):
        """Load backup history from database"""
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT metadata FROM synapsedb_backups.backup_metadata
                    WHERE coordinator_node = $1
                    ORDER BY start_time DESC
                    LIMIT 100
                """, self.node_id)
                
                for row in rows:
                    metadata_dict = json.loads(row['metadata'])
                    backup_metadata = BackupMetadata.from_dict(metadata_dict)
                    
                    if backup_metadata.status in [BackupStatus.PENDING, BackupStatus.RUNNING]:
                        self.active_backups[backup_metadata.backup_id] = backup_metadata
                    else:
                        self.backup_history.append(backup_metadata)
                
                logger.info(f"Loaded {len(self.backup_history)} backup records, "
                           f"{len(self.active_backups)} active backups")
                
        except Exception as e:
            logger.error(f"Failed to load backup history: {e}")
    
    def get_backup_status(self) -> Dict[str, Any]:
        """Get overall backup system status"""
        return {
            'node_id': self.node_id,
            'storage_type': self.storage_config.get('type', 'unknown'),
            'active_backups': len(self.active_backups),
            'backup_history_count': len(self.backup_history),
            'wal_archive_enabled': self.wal_archive_enabled,
            'retention_days': self.backup_retention_days,
            'recent_backups': [
                {
                    'backup_id': backup.backup_id,
                    'backup_type': backup.backup_type.value,
                    'status': backup.status.value,
                    'start_time': backup.start_time.isoformat(),
                    'size_mb': backup.size_bytes // 1024 // 1024
                }
                for backup in sorted(self.backup_history, key=lambda b: b.start_time, reverse=True)[:10]
            ]
        }