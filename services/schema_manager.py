"""
SynapseDB Schema Manager
Coordinated schema versioning and migration system using Raft consensus
"""

import asyncio
import asyncpg
import logging
import json
import hashlib
import time
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)

class MigrationStatus(Enum):
    PENDING = "pending"
    PREPARING = "preparing"
    APPLYING = "applying"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"

@dataclass
class SchemaMigration:
    migration_id: str
    version: int
    name: str
    up_sql: str
    down_sql: str
    checksum: str
    dependencies: List[str]  # Migration IDs this depends on
    affects_tables: List[str]
    status: MigrationStatus
    created_at: float
    applied_at: Optional[float] = None
    applied_by_node: Optional[str] = None
    error_message: Optional[str] = None

@dataclass 
class SchemaVersion:
    version: int
    migrations: List[str]  # Migration IDs in this version
    applied_migrations: Set[str]  # Successfully applied migrations
    schema_checksum: str
    created_at: float

class SchemaManager:
    """Manages schema versioning and coordinated migrations"""
    
    def __init__(self, node_id: str, db_config: Dict, raft_node, cluster_nodes: List[Dict]):
        self.node_id = node_id
        self.db_config = db_config
        self.raft_node = raft_node  # Reference to Raft consensus node
        self.cluster_nodes = {n['id']: n for n in cluster_nodes}
        
        # Database connection
        self.db_pool: Optional[asyncpg.Pool] = None
        
        # Schema state
        self.current_version = 0
        self.pending_migrations: Dict[str, SchemaMigration] = {}
        self.applied_migrations: Dict[str, SchemaMigration] = {}
        self.schema_versions: Dict[int, SchemaVersion] = {}
        
        # Migration coordination
        self.migration_lock = asyncio.Lock()
        self.running = False
        
        # Migration timeout
        self.migration_timeout = 300  # 5 minutes
    
    async def initialize(self):
        """Initialize schema manager"""
        logger.info(f"Initializing schema manager for node {self.node_id}")
        
        self.db_pool = await asyncpg.create_pool(**self.db_config, min_size=2, max_size=8)
        
        # Create schema management tables
        await self._create_schema_tables()
        
        # Load current schema state
        await self._load_schema_state()
        
        logger.info(f"Schema manager initialized - current version: {self.current_version}")
    
    async def start(self):
        """Start schema manager"""
        self.running = True
        logger.info("Starting schema manager")
        
        tasks = [
            asyncio.create_task(self._migration_processing_loop()),
            asyncio.create_task(self._schema_sync_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Schema manager error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop schema manager"""
        self.running = False
        logger.info("Stopping schema manager")
        
        if self.db_pool:
            await self.db_pool.close()
    
    async def _create_schema_tables(self):
        """Create schema management tables"""
        try:
            async with self.db_pool.acquire() as conn:
                # Schema migrations table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS synapsedb_schema.migrations (
                        migration_id TEXT PRIMARY KEY,
                        version INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        up_sql TEXT NOT NULL,
                        down_sql TEXT NOT NULL,
                        checksum TEXT NOT NULL,
                        dependencies TEXT[], -- JSON array of migration IDs
                        affects_tables TEXT[],
                        status TEXT NOT NULL DEFAULT 'pending',
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        applied_at TIMESTAMPTZ,
                        applied_by_node UUID,
                        error_message TEXT
                    )
                """)
                
                # Schema versions table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS synapsedb_schema.schema_versions (
                        version INTEGER PRIMARY KEY,
                        migrations TEXT[], -- Migration IDs in this version
                        applied_migrations TEXT[], -- Successfully applied migrations
                        schema_checksum TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                
                # Schema locks table for coordination
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS synapsedb_schema.schema_locks (
                        lock_name TEXT PRIMARY KEY,
                        locked_by_node UUID NOT NULL,
                        migration_id TEXT,
                        acquired_at TIMESTAMPTZ DEFAULT NOW(),
                        expires_at TIMESTAMPTZ NOT NULL
                    )
                """)
                
                # Create indexes
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_migrations_version ON synapsedb_schema.migrations(version)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_migrations_status ON synapsedb_schema.migrations(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_schema_locks_expires ON synapsedb_schema.schema_locks(expires_at)")
                
        except Exception as e:
            logger.error(f"Failed to create schema tables: {e}")
            raise
    
    async def _load_schema_state(self):
        """Load current schema state from database"""
        try:
            async with self.db_pool.acquire() as conn:
                # Load migrations
                migration_rows = await conn.fetch("""
                    SELECT migration_id, version, name, up_sql, down_sql, checksum,
                           dependencies, affects_tables, status, 
                           EXTRACT(EPOCH FROM created_at) as created_at,
                           EXTRACT(EPOCH FROM applied_at) as applied_at,
                           applied_by_node, error_message
                    FROM synapsedb_schema.migrations
                    ORDER BY version, created_at
                """)
                
                for row in migration_rows:
                    migration = SchemaMigration(
                        migration_id=row['migration_id'],
                        version=row['version'],
                        name=row['name'],
                        up_sql=row['up_sql'],
                        down_sql=row['down_sql'],
                        checksum=row['checksum'],
                        dependencies=row['dependencies'] or [],
                        affects_tables=row['affects_tables'] or [],
                        status=MigrationStatus(row['status']),
                        created_at=row['created_at'],
                        applied_at=row['applied_at'],
                        applied_by_node=str(row['applied_by_node']) if row['applied_by_node'] else None,
                        error_message=row['error_message']
                    )
                    
                    if migration.status == MigrationStatus.COMPLETED:
                        self.applied_migrations[migration.migration_id] = migration
                    else:
                        self.pending_migrations[migration.migration_id] = migration
                
                # Load schema versions
                version_rows = await conn.fetch("""
                    SELECT version, migrations, applied_migrations, schema_checksum,
                           EXTRACT(EPOCH FROM created_at) as created_at
                    FROM synapsedb_schema.schema_versions
                    ORDER BY version DESC
                """)
                
                for row in version_rows:
                    schema_version = SchemaVersion(
                        version=row['version'],
                        migrations=row['migrations'] or [],
                        applied_migrations=set(row['applied_migrations'] or []),
                        schema_checksum=row['schema_checksum'],
                        created_at=row['created_at']
                    )
                    self.schema_versions[schema_version.version] = schema_version
                
                # Set current version
                if self.schema_versions:
                    self.current_version = max(self.schema_versions.keys())
                
                logger.info(f"Loaded {len(self.applied_migrations)} applied migrations, "
                          f"{len(self.pending_migrations)} pending migrations")
                
        except Exception as e:
            logger.error(f"Failed to load schema state: {e}")
    
    async def create_migration(self, name: str, up_sql: str, down_sql: str, 
                             affects_tables: List[str] = None, 
                             dependencies: List[str] = None) -> str:
        """Create a new schema migration"""
        try:
            # Generate migration ID
            content = f"{name}:{up_sql}:{down_sql}"
            migration_id = f"migration_{int(time.time())}_{hashlib.md5(content.encode()).hexdigest()[:8]}"
            
            # Calculate checksum
            checksum = hashlib.sha256(content.encode()).hexdigest()
            
            # Determine version (next version)
            next_version = self.current_version + 1
            
            # Create migration object
            migration = SchemaMigration(
                migration_id=migration_id,
                version=next_version,
                name=name,
                up_sql=up_sql,
                down_sql=down_sql,
                checksum=checksum,
                dependencies=dependencies or [],
                affects_tables=affects_tables or [],
                status=MigrationStatus.PENDING,
                created_at=time.time()
            )
            
            # Store in database via Raft consensus
            if self.raft_node.is_leader():
                command_data = {
                    'type': 'create_migration',
                    'migration': asdict(migration)
                }
                
                success = await self.raft_node.submit_command(command_data)
                if success:
                    self.pending_migrations[migration_id] = migration
                    logger.info(f"Created migration {migration_id}: {name}")
                    return migration_id
                else:
                    raise Exception("Failed to submit migration via Raft")
            else:
                raise Exception("Only leader can create migrations")
                
        except Exception as e:
            logger.error(f"Failed to create migration: {e}")
            raise
    
    async def apply_migration(self, migration_id: str) -> bool:
        """Apply a specific migration"""
        try:
            async with self.migration_lock:
                if migration_id not in self.pending_migrations:
                    logger.error(f"Migration {migration_id} not found or already applied")
                    return False
                
                migration = self.pending_migrations[migration_id]
                
                # Check dependencies
                if not await self._check_dependencies(migration):
                    logger.error(f"Migration {migration_id} dependencies not satisfied")
                    return False
                
                # Update status to applying
                migration.status = MigrationStatus.APPLYING
                migration.applied_by_node = self.node_id
                await self._persist_migration(migration)
                
                logger.info(f"Applying migration {migration_id}: {migration.name}")
                
                # Execute migration on all nodes via Raft
                if self.raft_node.is_leader():
                    command_data = {
                        'type': 'apply_migration',
                        'migration_id': migration_id,
                        'sql': migration.up_sql
                    }
                    
                    success = await self.raft_node.submit_command(command_data)
                    
                    if success:
                        # Mark as completed
                        migration.status = MigrationStatus.COMPLETED
                        migration.applied_at = time.time()
                        
                        # Move from pending to applied
                        self.applied_migrations[migration_id] = migration
                        del self.pending_migrations[migration_id]
                        
                        await self._persist_migration(migration)
                        
                        # Update schema version
                        await self._update_schema_version()
                        
                        logger.info(f"Successfully applied migration {migration_id}")
                        return True
                    else:
                        migration.status = MigrationStatus.FAILED
                        migration.error_message = "Raft consensus failed"
                        await self._persist_migration(migration)
                        return False
                else:
                    # Redirect to leader
                    leader_id = self.raft_node.get_leader_id()
                    logger.info(f"Redirecting migration request to leader: {leader_id}")
                    return await self._forward_to_leader('apply_migration', {'migration_id': migration_id})
                    
        except Exception as e:
            logger.error(f"Failed to apply migration {migration_id}: {e}")
            
            # Mark as failed
            if migration_id in self.pending_migrations:
                migration = self.pending_migrations[migration_id]
                migration.status = MigrationStatus.FAILED
                migration.error_message = str(e)
                await self._persist_migration(migration)
            
            return False
    
    async def rollback_migration(self, migration_id: str) -> bool:
        """Rollback a specific migration"""
        try:
            async with self.migration_lock:
                if migration_id not in self.applied_migrations:
                    logger.error(f"Migration {migration_id} not found or not applied")
                    return False
                
                migration = self.applied_migrations[migration_id]
                
                logger.info(f"Rolling back migration {migration_id}: {migration.name}")
                
                # Execute rollback via Raft
                if self.raft_node.is_leader():
                    command_data = {
                        'type': 'rollback_migration',
                        'migration_id': migration_id,
                        'sql': migration.down_sql
                    }
                    
                    success = await self.raft_node.submit_command(command_data)
                    
                    if success:
                        # Mark as rolled back
                        migration.status = MigrationStatus.ROLLED_BACK
                        
                        # Move from applied to pending
                        self.pending_migrations[migration_id] = migration
                        del self.applied_migrations[migration_id]
                        
                        await self._persist_migration(migration)
                        await self._update_schema_version()
                        
                        logger.info(f"Successfully rolled back migration {migration_id}")
                        return True
                    else:
                        logger.error(f"Failed to rollback migration {migration_id} via Raft")
                        return False
                else:
                    leader_id = self.raft_node.get_leader_id()
                    logger.info(f"Redirecting rollback request to leader: {leader_id}")
                    return await self._forward_to_leader('rollback_migration', {'migration_id': migration_id})
                    
        except Exception as e:
            logger.error(f"Failed to rollback migration {migration_id}: {e}")
            return False
    
    async def _check_dependencies(self, migration: SchemaMigration) -> bool:
        """Check if migration dependencies are satisfied"""
        for dep_id in migration.dependencies:
            if dep_id not in self.applied_migrations:
                logger.warning(f"Dependency {dep_id} not satisfied for migration {migration.migration_id}")
                return False
        return True
    
    async def _persist_migration(self, migration: SchemaMigration):
        """Persist migration to database"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synapsedb_schema.migrations 
                    (migration_id, version, name, up_sql, down_sql, checksum, 
                     dependencies, affects_tables, status, created_at, applied_at, 
                     applied_by_node, error_message)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 
                            to_timestamp($10), to_timestamp($11), $12, $13)
                    ON CONFLICT (migration_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        applied_at = EXCLUDED.applied_at,
                        applied_by_node = EXCLUDED.applied_by_node,
                        error_message = EXCLUDED.error_message
                """, migration.migration_id, migration.version, migration.name,
                    migration.up_sql, migration.down_sql, migration.checksum,
                    migration.dependencies, migration.affects_tables,
                    migration.status.value, migration.created_at,
                    migration.applied_at, migration.applied_by_node, migration.error_message)
                    
        except Exception as e:
            logger.error(f"Failed to persist migration {migration.migration_id}: {e}")
    
    async def _update_schema_version(self):
        """Update schema version after migration changes"""
        try:
            # Calculate new version info
            applied_migration_ids = list(self.applied_migrations.keys())
            
            # Calculate schema checksum based on applied migrations
            checksum_content = "|".join(sorted(applied_migration_ids))
            schema_checksum = hashlib.sha256(checksum_content.encode()).hexdigest()
            
            # Create new schema version
            new_version = self.current_version + 1
            schema_version = SchemaVersion(
                version=new_version,
                migrations=applied_migration_ids,
                applied_migrations=set(applied_migration_ids),
                schema_checksum=schema_checksum,
                created_at=time.time()
            )
            
            # Store in database
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synapsedb_schema.schema_versions
                    (version, migrations, applied_migrations, schema_checksum, created_at)
                    VALUES ($1, $2, $3, $4, to_timestamp($5))
                """, schema_version.version, schema_version.migrations, 
                    list(schema_version.applied_migrations), schema_version.schema_checksum,
                    schema_version.created_at)
            
            # Update local state
            self.schema_versions[new_version] = schema_version
            self.current_version = new_version
            
            logger.info(f"Updated schema version to {new_version}")
            
        except Exception as e:
            logger.error(f"Failed to update schema version: {e}")
    
    async def _migration_processing_loop(self):
        """Process pending migrations automatically"""
        while self.running:
            try:
                if self.raft_node.is_leader():
                    # Auto-apply migrations that have all dependencies satisfied
                    for migration_id, migration in self.pending_migrations.copy().items():
                        if migration.status == MigrationStatus.PENDING:
                            if await self._check_dependencies(migration):
                                logger.info(f"Auto-applying migration {migration_id}")
                                await self.apply_migration(migration_id)
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in migration processing loop: {e}")
                await asyncio.sleep(10)
    
    async def _schema_sync_loop(self):
        """Sync schema state across cluster"""
        while self.running:
            try:
                # Check for schema consistency across nodes
                await self._verify_schema_consistency()
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in schema sync loop: {e}")
                await asyncio.sleep(30)
    
    async def _verify_schema_consistency(self):
        """Verify schema consistency across cluster"""
        try:
            # This would check that all nodes have the same applied migrations
            # For now, just log current state
            logger.debug(f"Schema verification - Version: {self.current_version}, "
                        f"Applied migrations: {len(self.applied_migrations)}")
                        
        except Exception as e:
            logger.error(f"Failed to verify schema consistency: {e}")
    
    async def _forward_to_leader(self, operation: str, params: Dict) -> bool:
        """Forward operation to Raft leader"""
        try:
            leader_id = self.raft_node.get_leader_id()
            if not leader_id or leader_id not in self.cluster_nodes:
                return False
            
            leader_info = self.cluster_nodes[leader_id]
            
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = f"http://{leader_info['host']}:{leader_info.get('schema_port', 8082)}/schema/{operation}"
                async with session.post(url, json=params) as response:
                    return response.status == 200
                    
        except Exception as e:
            logger.error(f"Failed to forward to leader: {e}")
            return False
    
    async def get_schema_status(self) -> Dict:
        """Get comprehensive schema status"""
        return {
            'current_version': self.current_version,
            'applied_migrations': len(self.applied_migrations),
            'pending_migrations': len(self.pending_migrations),
            'is_leader': self.raft_node.is_leader(),
            'leader_id': self.raft_node.get_leader_id(),
            'migrations': {
                'applied': [
                    {
                        'id': m.migration_id,
                        'name': m.name,
                        'version': m.version,
                        'applied_at': m.applied_at
                    } for m in self.applied_migrations.values()
                ],
                'pending': [
                    {
                        'id': m.migration_id,
                        'name': m.name,
                        'version': m.version,
                        'status': m.status.value
                    } for m in self.pending_migrations.values()
                ]
            }
        }