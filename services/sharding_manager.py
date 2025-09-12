"""
SynapseDB Sharding Manager
Implements automatic data partitioning and shard management across cluster nodes
"""

import asyncio
import asyncpg
import logging
import hashlib
import json
import time
from typing import Dict, List, Optional, Tuple, Set, Any
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class ShardingStrategy(Enum):
    HASH = "hash"
    RANGE = "range"
    DIRECTORY = "directory"

@dataclass
class ShardInfo:
    shard_id: int
    strategy: ShardingStrategy
    key_column: str
    key_range_start: Optional[str] = None
    key_range_end: Optional[str] = None
    primary_node_id: str = None
    replica_node_ids: List[str] = None
    table_name: str = None
    status: str = "active"  # active, migrating, inactive
    
    def __post_init__(self):
        if self.replica_node_ids is None:
            self.replica_node_ids = []

@dataclass
class ShardMigration:
    migration_id: str
    shard_id: int
    source_node_id: str
    target_node_id: str
    status: str  # pending, in_progress, completed, failed
    started_at: float
    completed_at: Optional[float] = None
    error_message: Optional[str] = None

class ShardingManager:
    """Manages data sharding and distribution across cluster nodes"""
    
    def __init__(self, node_id: str, db_config: Dict, cluster_nodes: List[Dict], 
                 total_shards: int = 256, replication_factor: int = 3):
        self.node_id = node_id
        self.db_config = db_config
        self.cluster_nodes = {n['id']: n for n in cluster_nodes}
        self.total_shards = total_shards
        self.replication_factor = replication_factor
        
        # Database connection
        self.db_pool: Optional[asyncpg.Pool] = None
        
        # Shard mappings
        self.shard_map: Dict[int, ShardInfo] = {}
        self.table_shards: Dict[str, List[int]] = {}  # table -> list of shard_ids
        self.node_shards: Dict[str, Set[int]] = {}    # node_id -> set of shard_ids
        
        # Migration tracking
        self.active_migrations: Dict[str, ShardMigration] = {}
        
        # Running state
        self.running = False
        
        # Load balancing thresholds
        self.load_imbalance_threshold = 0.2  # 20% imbalance triggers rebalancing
        self.shard_size_threshold = 1024 * 1024 * 1024  # 1GB per shard
    
    async def initialize(self):
        """Initialize the sharding manager"""
        logger.info(f"Initializing sharding manager for node {self.node_id}")
        
        # Connect to database
        self.db_pool = await asyncpg.create_pool(**self.db_config, min_size=3, max_size=15)
        
        # Load existing shard assignments
        await self._load_shard_assignments()
        
        # Initialize node shard mappings
        await self._build_node_shard_mappings()
        
        # Create shard assignment if this is the first node
        if not self.shard_map:
            await self._initialize_shard_assignments()
        
        logger.info(f"Sharding manager initialized with {len(self.shard_map)} shards")
    
    async def start(self):
        """Start the sharding manager"""
        self.running = True
        logger.info("Starting sharding manager")
        
        tasks = [
            asyncio.create_task(self._shard_monitoring_loop()),
            asyncio.create_task(self._rebalancing_loop()),
            asyncio.create_task(self._migration_processing_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Sharding manager error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the sharding manager"""
        self.running = False
        logger.info("Stopping sharding manager")
        
        if self.db_pool:
            await self.db_pool.close()
    
    async def _load_shard_assignments(self):
        """Load shard assignments from database"""
        try:
            async with self.db_pool.acquire() as conn:
                shards = await conn.fetch("""
                    SELECT shard_id, primary_node_id, replica_node_ids, status,
                           key_range_start, key_range_end, table_name
                    FROM synapsedb_sharding.shard_assignments
                    ORDER BY shard_id
                """)
                
                for shard_row in shards:
                    shard_info = ShardInfo(
                        shard_id=shard_row['shard_id'],
                        strategy=ShardingStrategy.HASH,  # Default strategy
                        key_column='id',  # Default key column
                        key_range_start=shard_row['key_range_start'],
                        key_range_end=shard_row['key_range_end'],
                        primary_node_id=str(shard_row['primary_node_id']),
                        replica_node_ids=[str(nid) for nid in shard_row['replica_node_ids']],
                        table_name=shard_row['table_name'],
                        status=shard_row['status']
                    )
                    
                    self.shard_map[shard_info.shard_id] = shard_info
                
                logger.info(f"Loaded {len(self.shard_map)} shard assignments")
                
        except Exception as e:
            logger.error(f"Failed to load shard assignments: {e}")
    
    async def _initialize_shard_assignments(self):
        """Initialize shard assignments for a new cluster"""
        try:
            logger.info(f"Initializing {self.total_shards} shards across {len(self.cluster_nodes)} nodes")
            
            available_nodes = list(self.cluster_nodes.keys())
            if len(available_nodes) < self.replication_factor:
                logger.warning(f"Only {len(available_nodes)} nodes available, but replication factor is {self.replication_factor}")
                self.replication_factor = len(available_nodes)
            
            async with self.db_pool.acquire() as conn:
                for shard_id in range(self.total_shards):
                    # Assign primary node using consistent hashing
                    primary_idx = shard_id % len(available_nodes)
                    primary_node = available_nodes[primary_idx]
                    
                    # Assign replica nodes (next N nodes in ring)
                    replica_nodes = []
                    for i in range(1, self.replication_factor):
                        replica_idx = (primary_idx + i) % len(available_nodes)
                        replica_nodes.append(available_nodes[replica_idx])
                    
                    # Calculate key range for hash-based sharding
                    key_range_start = str(shard_id)
                    key_range_end = str(shard_id)
                    
                    # Insert shard assignment
                    await conn.execute("""
                        INSERT INTO synapsedb_sharding.shard_assignments 
                        (shard_id, primary_node_id, replica_node_ids, status, 
                         key_range_start, key_range_end, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, NOW())
                    """, shard_id, primary_node, replica_nodes, 'active', 
                        key_range_start, key_range_end)
                    
                    # Create shard info
                    shard_info = ShardInfo(
                        shard_id=shard_id,
                        strategy=ShardingStrategy.HASH,
                        key_column='id',
                        key_range_start=key_range_start,
                        key_range_end=key_range_end,
                        primary_node_id=primary_node,
                        replica_node_ids=replica_nodes,
                        status='active'
                    )
                    
                    self.shard_map[shard_id] = shard_info
            
            logger.info(f"Initialized {self.total_shards} shards")
            
        except Exception as e:
            logger.error(f"Failed to initialize shard assignments: {e}")
            raise
    
    async def _build_node_shard_mappings(self):
        """Build node to shard mappings for quick lookup"""
        self.node_shards = {node_id: set() for node_id in self.cluster_nodes.keys()}
        
        for shard_info in self.shard_map.values():
            if shard_info.primary_node_id in self.node_shards:
                self.node_shards[shard_info.primary_node_id].add(shard_info.shard_id)
            
            for replica_node in shard_info.replica_node_ids:
                if replica_node in self.node_shards:
                    self.node_shards[replica_node].add(shard_info.shard_id)
    
    async def _shard_monitoring_loop(self):
        """Monitor shard health and distribution"""
        while self.running:
            try:
                await self._update_shard_statistics()
                await self._check_shard_health()
                
                await asyncio.sleep(30)  # Monitor every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in shard monitoring loop: {e}")
                await asyncio.sleep(10)
    
    async def _rebalancing_loop(self):
        """Monitor cluster balance and trigger rebalancing"""
        while self.running:
            try:
                balance_score = await self._calculate_cluster_balance()
                
                if balance_score > self.load_imbalance_threshold:
                    logger.info(f"Cluster imbalance detected (score: {balance_score:.2f}), triggering rebalancing")
                    await self._trigger_rebalancing()
                
                await asyncio.sleep(300)  # Check balance every 5 minutes
                
            except Exception as e:
                logger.error(f"Error in rebalancing loop: {e}")
                await asyncio.sleep(60)
    
    async def _migration_processing_loop(self):
        """Process shard migrations"""
        while self.running:
            try:
                await self._process_pending_migrations()
                await self._check_migration_progress()
                
                await asyncio.sleep(10)  # Check migrations every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in migration processing loop: {e}")
                await asyncio.sleep(5)
    
    def calculate_shard_id(self, table_name: str, key_value: Any, strategy: ShardingStrategy = ShardingStrategy.HASH) -> int:
        """Calculate shard ID for a given table and key value"""
        if strategy == ShardingStrategy.HASH:
            # Use consistent hashing
            key_str = f"{table_name}:{str(key_value)}"
            hash_value = int(hashlib.md5(key_str.encode()).hexdigest(), 16)
            return hash_value % self.total_shards
        
        elif strategy == ShardingStrategy.RANGE:
            # Range-based sharding (simplified)
            # Would need more sophisticated range mapping in production
            return 0  # Placeholder
        
        elif strategy == ShardingStrategy.DIRECTORY:
            # Directory-based sharding
            # Would look up in directory service
            return 0  # Placeholder
        
        return 0
    
    def get_shard_nodes(self, shard_id: int) -> Tuple[str, List[str]]:
        """Get primary and replica nodes for a shard"""
        if shard_id not in self.shard_map:
            raise ValueError(f"Shard {shard_id} not found")
        
        shard_info = self.shard_map[shard_id]
        return shard_info.primary_node_id, shard_info.replica_node_ids
    
    def get_nodes_for_key(self, table_name: str, key_value: Any) -> Tuple[str, List[str]]:
        """Get nodes that should contain data for a given key"""
        shard_id = self.calculate_shard_id(table_name, key_value)
        return self.get_shard_nodes(shard_id)
    
    async def create_sharded_table(self, table_name: str, table_schema: str, 
                                  shard_key: str = 'id', strategy: ShardingStrategy = ShardingStrategy.HASH):
        """Create a sharded table across the cluster"""
        try:
            logger.info(f"Creating sharded table {table_name} with shard key {shard_key}")
            
            # Determine which shards will contain this table's data
            table_shard_ids = []
            
            if strategy == ShardingStrategy.HASH:
                # All shards can potentially contain data for hash-based sharding
                table_shard_ids = list(range(self.total_shards))
            else:
                # For range/directory sharding, would determine specific shards
                table_shard_ids = list(range(self.total_shards))
            
            # Create table on nodes that host the relevant shards
            nodes_to_create = set()
            for shard_id in table_shard_ids:
                shard_info = self.shard_map[shard_id]
                nodes_to_create.add(shard_info.primary_node_id)
                nodes_to_create.extend(shard_info.replica_node_ids)
            
            # Create table on all relevant nodes
            creation_tasks = []
            for node_id in nodes_to_create:
                if node_id in self.cluster_nodes:
                    task = asyncio.create_task(
                        self._create_table_on_node(node_id, table_name, table_schema, shard_key)
                    )
                    creation_tasks.append(task)
            
            results = await asyncio.gather(*creation_tasks, return_exceptions=True)
            
            # Check results
            successful_creates = sum(1 for r in results if r is True)
            logger.info(f"Created table {table_name} on {successful_creates}/{len(creation_tasks)} nodes")
            
            # Update table shard mapping
            self.table_shards[table_name] = table_shard_ids
            
            # Update shard assignments in database
            async with self.db_pool.acquire() as conn:
                for shard_id in table_shard_ids:
                    await conn.execute("""
                        UPDATE synapsedb_sharding.shard_assignments 
                        SET table_name = CASE 
                            WHEN table_name IS NULL THEN $1
                            WHEN table_name NOT LIKE '%' || $1 || '%' THEN table_name || ',' || $1
                            ELSE table_name
                        END,
                        updated_at = NOW()
                        WHERE shard_id = $2
                    """, table_name, shard_id)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create sharded table {table_name}: {e}")
            return False
    
    async def _create_table_on_node(self, node_id: str, table_name: str, table_schema: str, shard_key: str) -> bool:
        """Create table on a specific node"""
        try:
            node_info = self.cluster_nodes[node_id]
            
            # Connect to the node
            node_config = self.db_config.copy()
            node_config['host'] = node_info['host']
            node_config['port'] = node_info['port']
            
            async with asyncpg.create_pool(**node_config, min_size=1, max_size=2) as pool:
                async with pool.acquire() as conn:
                    # Add sharding metadata columns to schema
                    enhanced_schema = table_schema.rstrip(');') + ","
                    enhanced_schema += """
                        shard_id INTEGER,
                        last_updated_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_by_node UUID,
                        vector_clock JSONB DEFAULT '{}',
                        CONSTRAINT shard_check CHECK (shard_id >= 0 AND shard_id < 256)
                    );
                    """
                    
                    # Create the table
                    await conn.execute(enhanced_schema)
                    
                    # Create shard-aware indexes
                    await conn.execute(f"""
                        CREATE INDEX IF NOT EXISTS idx_{table_name.replace('.', '_')}_shard 
                        ON {table_name} (shard_id, {shard_key})
                    """)
                    
                    # Create triggers for automatic shard assignment and metadata
                    await conn.execute(f"""
                        CREATE OR REPLACE FUNCTION {table_name.replace('.', '_')}_shard_trigger()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            -- Calculate shard ID
                            NEW.shard_id := (
                                abs(hashtext((TG_TABLE_NAME || ':' || NEW.{shard_key})::text)) % 256
                            );
                            
                            -- Update metadata
                            NEW.last_updated_at := NOW();
                            NEW.updated_by_node := current_setting('synapsedb.node_id', true)::UUID;
                            
                            -- Update vector clock (simplified)
                            NEW.vector_clock := jsonb_set(
                                COALESCE(NEW.vector_clock, '{{}}'::jsonb),
                                ARRAY[current_setting('synapsedb.node_id', true)],
                                to_jsonb(extract(epoch from now()) * 1000)
                            );
                            
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;
                    """)
                    
                    await conn.execute(f"""
                        CREATE TRIGGER {table_name.replace('.', '_')}_shard_assign
                        BEFORE INSERT OR UPDATE ON {table_name}
                        FOR EACH ROW EXECUTE FUNCTION {table_name.replace('.', '_')}_shard_trigger()
                    """)
                    
                    logger.info(f"Created sharded table {table_name} on node {node_id}")
                    return True
        
        except Exception as e:
            logger.error(f"Failed to create table {table_name} on node {node_id}: {e}")
            return False
    
    async def _update_shard_statistics(self):
        """Update statistics for all shards"""
        try:
            async with self.db_pool.acquire() as conn:
                # This would collect detailed statistics about each shard
                # For now, just update basic info
                for shard_id, shard_info in self.shard_map.items():
                    # Update last check time or other metadata
                    pass
        
        except Exception as e:
            logger.error(f"Failed to update shard statistics: {e}")
    
    async def _check_shard_health(self):
        """Check health of all shards"""
        try:
            # Check if any shards have unavailable nodes
            for shard_id, shard_info in self.shard_map.items():
                unavailable_nodes = []
                
                # Check primary node
                if shard_info.primary_node_id not in self.cluster_nodes:
                    unavailable_nodes.append(shard_info.primary_node_id)
                
                # Check replica nodes
                for replica_node in shard_info.replica_node_ids:
                    if replica_node not in self.cluster_nodes:
                        unavailable_nodes.append(replica_node)
                
                if unavailable_nodes:
                    logger.warning(f"Shard {shard_id} has unavailable nodes: {unavailable_nodes}")
                    # Would trigger shard recovery/rebalancing
        
        except Exception as e:
            logger.error(f"Failed to check shard health: {e}")
    
    async def _calculate_cluster_balance(self) -> float:
        """Calculate cluster balance score (0.0 = perfect balance, 1.0 = completely imbalanced)"""
        try:
            if not self.node_shards:
                return 0.0
            
            shard_counts = [len(shards) for shards in self.node_shards.values()]
            
            if not shard_counts:
                return 0.0
            
            avg_shards = sum(shard_counts) / len(shard_counts)
            max_deviation = max(abs(count - avg_shards) for count in shard_counts)
            
            # Normalize by average to get a ratio
            balance_score = max_deviation / avg_shards if avg_shards > 0 else 0.0
            
            return min(balance_score, 1.0)  # Cap at 1.0
            
        except Exception as e:
            logger.error(f"Failed to calculate cluster balance: {e}")
            return 0.0
    
    async def _trigger_rebalancing(self):
        """Trigger cluster rebalancing"""
        try:
            logger.info("Starting cluster rebalancing")
            
            # Calculate ideal shard distribution
            total_shards = len(self.shard_map)
            active_nodes = list(self.cluster_nodes.keys())
            ideal_shards_per_node = total_shards / len(active_nodes)
            
            # Find nodes that are over/under loaded
            overloaded_nodes = []
            underloaded_nodes = []
            
            for node_id, shards in self.node_shards.items():
                current_count = len(shards)
                if current_count > ideal_shards_per_node * (1 + self.load_imbalance_threshold):
                    overloaded_nodes.append((node_id, current_count))
                elif current_count < ideal_shards_per_node * (1 - self.load_imbalance_threshold):
                    underloaded_nodes.append((node_id, current_count))
            
            # Plan migrations from overloaded to underloaded nodes
            migrations_planned = 0
            for overloaded_node, _ in overloaded_nodes:
                for underloaded_node, _ in underloaded_nodes:
                    # Find a shard to migrate
                    candidate_shards = self.node_shards[overloaded_node]
                    if candidate_shards and migrations_planned < 5:  # Limit concurrent migrations
                        shard_to_migrate = next(iter(candidate_shards))
                        await self._plan_shard_migration(shard_to_migrate, overloaded_node, underloaded_node)
                        migrations_planned += 1
            
            logger.info(f"Planned {migrations_planned} shard migrations for rebalancing")
            
        except Exception as e:
            logger.error(f"Failed to trigger rebalancing: {e}")
    
    async def _plan_shard_migration(self, shard_id: int, source_node: str, target_node: str):
        """Plan a shard migration"""
        try:
            migration_id = f"migration_{shard_id}_{source_node}_to_{target_node}_{int(time.time())}"
            
            migration = ShardMigration(
                migration_id=migration_id,
                shard_id=shard_id,
                source_node_id=source_node,
                target_node_id=target_node,
                status='pending',
                started_at=time.time()
            )
            
            self.active_migrations[migration_id] = migration
            
            # Store migration plan in database
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synapsedb_sharding.shard_migrations
                    (migration_id, shard_id, source_node_id, target_node_id, status, started_at)
                    VALUES ($1, $2, $3, $4, $5, to_timestamp($6))
                """, migration_id, shard_id, source_node, target_node, 'pending', migration.started_at)
            
            logger.info(f"Planned migration {migration_id}: shard {shard_id} from {source_node} to {target_node}")
            
        except Exception as e:
            logger.error(f"Failed to plan shard migration: {e}")
    
    async def _process_pending_migrations(self):
        """Process pending shard migrations"""
        try:
            pending_migrations = [m for m in self.active_migrations.values() if m.status == 'pending']
            
            for migration in pending_migrations[:2]:  # Process max 2 migrations concurrently
                logger.info(f"Starting migration {migration.migration_id}")
                migration.status = 'in_progress'
                
                # Start migration in background
                asyncio.create_task(self._execute_shard_migration(migration))
        
        except Exception as e:
            logger.error(f"Failed to process pending migrations: {e}")
    
    async def _execute_shard_migration(self, migration: ShardMigration):
        """Execute a shard migration"""
        try:
            logger.info(f"Executing migration {migration.migration_id}")
            
            # This would involve:
            # 1. Copy shard data from source to target
            # 2. Update shard assignments
            # 3. Switch traffic to new node
            # 4. Clean up old data
            
            # For now, just simulate the migration
            await asyncio.sleep(10)  # Simulate migration time
            
            # Update shard assignment
            shard_info = self.shard_map[migration.shard_id]
            
            # Replace source node with target node in assignment
            if shard_info.primary_node_id == migration.source_node_id:
                shard_info.primary_node_id = migration.target_node_id
            elif migration.source_node_id in shard_info.replica_node_ids:
                shard_info.replica_node_ids.remove(migration.source_node_id)
                shard_info.replica_node_ids.append(migration.target_node_id)
            
            # Update database
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE synapsedb_sharding.shard_assignments
                    SET primary_node_id = $1, replica_node_ids = $2, updated_at = NOW()
                    WHERE shard_id = $3
                """, shard_info.primary_node_id, shard_info.replica_node_ids, migration.shard_id)
            
            # Update migration status
            migration.status = 'completed'
            migration.completed_at = time.time()
            
            # Update node shard mappings
            await self._build_node_shard_mappings()
            
            logger.info(f"Completed migration {migration.migration_id}")
            
        except Exception as e:
            migration.status = 'failed'
            migration.error_message = str(e)
            logger.error(f"Failed to execute migration {migration.migration_id}: {e}")
        
        finally:
            # Update database with final status
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE synapsedb_sharding.shard_migrations
                        SET status = $1, completed_at = to_timestamp($2), error_message = $3
                        WHERE migration_id = $4
                    """, migration.status, migration.completed_at or time.time(), migration.error_message, migration.migration_id)
            except:
                pass
    
    async def _check_migration_progress(self):
        """Check progress of active migrations"""
        try:
            # Clean up completed migrations
            completed_migrations = [
                mid for mid, migration in self.active_migrations.items() 
                if migration.status in ('completed', 'failed')
            ]
            
            for migration_id in completed_migrations:
                if time.time() - self.active_migrations[migration_id].started_at > 3600:  # Remove after 1 hour
                    del self.active_migrations[migration_id]
        
        except Exception as e:
            logger.error(f"Failed to check migration progress: {e}")
    
    async def get_shard_distribution(self) -> Dict:
        """Get current shard distribution across nodes"""
        try:
            distribution = {
                'total_shards': len(self.shard_map),
                'nodes': {},
                'balance_score': await self._calculate_cluster_balance(),
                'active_migrations': len([m for m in self.active_migrations.values() if m.status == 'in_progress'])
            }
            
            for node_id, shards in self.node_shards.items():
                node_info = self.cluster_nodes.get(node_id, {})
                distribution['nodes'][node_id] = {
                    'name': node_info.get('name', node_id),
                    'shard_count': len(shards),
                    'shard_ids': sorted(list(shards)),
                    'load_percentage': len(shards) / len(self.shard_map) * 100 if self.shard_map else 0
                }
            
            return distribution
            
        except Exception as e:
            logger.error(f"Failed to get shard distribution: {e}")
            return {'error': str(e)}