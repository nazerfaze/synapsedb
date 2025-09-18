"""
SynapseDB Replication Manager
Manages pglogical bi-directional replication setup and monitoring
"""

import asyncio
import asyncpg
import logging
from typing import Dict, List, Set, Optional, Tuple
import json
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class ReplicationManager:
    """Manages logical replication between PostgreSQL nodes"""
    
    def __init__(self, node_id: str, node_name: str, db_config: Dict, cluster_nodes: List[Dict], gossip_protocol=None):
        self.node_id = node_id
        self.node_name = node_name
        self.db_config = db_config
        self.cluster_nodes = {n['id']: n for n in cluster_nodes}
        self.gossip_protocol = gossip_protocol
        
        # Database connections
        self.local_pool: Optional[asyncpg.Pool] = None
        self.remote_pools: Dict[str, asyncpg.Pool] = {}
        
        # Replication state
        self.replication_sets: Set[str] = set()
        self.subscriptions: Dict[str, Dict] = {}  # target_node_id -> subscription_info
        self.publications: Dict[str, Dict] = {}   # publication_name -> info
        
        # Monitoring
        self.replication_lag_threshold = 60  # seconds
        self.running = False
        
        # Tables to replicate
        self.replicated_tables = set()
    
    async def initialize(self):
        """Initialize replication manager"""
        logger.info(f"Initializing replication manager for {self.node_name}")
        
        # Connect to local database
        self.local_pool = await asyncpg.create_pool(**self.db_config, min_size=3, max_size=15)
        
        # Initialize pglogical node
        await self._initialize_pglogical_node()

        # Discover existing replicated tables
        await self._discover_replicated_tables()

        # Set up auto-discovery hooks with gossip protocol
        if self.gossip_protocol:
            self._setup_auto_discovery_hooks()
        
        logger.info(f"Replication manager initialized for {self.node_name}")
    
    async def start(self):
        """Start replication management"""
        self.running = True
        logger.info(f"Starting replication manager for {self.node_name}")
        
        tasks = [
            asyncio.create_task(self._replication_setup_loop()),
            asyncio.create_task(self._replication_monitoring_loop()),
            asyncio.create_task(self._subscription_maintenance_loop()),
            asyncio.create_task(self._auto_discovery_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Replication manager error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop replication manager"""
        self.running = False
        logger.info(f"Stopping replication manager for {self.node_name}")
        
        # Close all connections
        if self.local_pool:
            await self.local_pool.close()
        
        for pool in self.remote_pools.values():
            await pool.close()
        
        self.remote_pools.clear()
    
    async def _initialize_pglogical_node(self):
        """Initialize this node as a pglogical node"""
        try:
            async with self.local_pool.acquire() as conn:
                # Check if node already exists
                node_exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pglogical.node WHERE node_name = $1)",
                    self.node_name
                )
                
                if not node_exists:
                    # Create pglogical node
                    node_dsn = f"host={self.db_config['host']} port={self.db_config['port']} dbname={self.db_config['database']} user={self.db_config['user']}"
                    
                    await conn.execute(
                        "SELECT pglogical.create_node(node_name := $1, dsn := $2)",
                        self.node_name, node_dsn
                    )
                    
                    logger.info(f"Created pglogical node: {self.node_name}")
                
                # Create default replication set
                replication_set_name = f"synapsedb_default_{self.node_name}"
                set_exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pglogical.replication_set WHERE set_name = $1)",
                    replication_set_name
                )
                
                if not set_exists:
                    await conn.execute(
                        "SELECT pglogical.create_replication_set($1)",
                        replication_set_name
                    )
                    logger.info(f"Created replication set: {replication_set_name}")
                
                self.replication_sets.add(replication_set_name)
                
        except Exception as e:
            logger.error(f"Failed to initialize pglogical node: {e}")
            raise
    
    async def _discover_replicated_tables(self):
        """Discover tables that should be replicated"""
        try:
            async with self.local_pool.acquire() as conn:
                # Get all user tables that have replication metadata columns
                tables_query = """
                    SELECT schemaname, tablename
                    FROM pg_tables
                    WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pglogical', 'synapsedb_replication', 'synapsedb_sharding', 'synapsedb_consensus')
                    AND EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = schemaname 
                        AND table_name = tablename 
                        AND column_name IN ('last_updated_at', 'updated_by_node')
                    )
                """
                
                tables = await conn.fetch(tables_query)
                
                for table in tables:
                    table_name = f"{table['schemaname']}.{table['tablename']}"
                    self.replicated_tables.add(table_name)
                
                logger.info(f"Discovered {len(self.replicated_tables)} tables for replication")
                
        except Exception as e:
            logger.error(f"Failed to discover replicated tables: {e}")
    
    async def _replication_setup_loop(self):
        """Main loop for setting up replication between nodes"""
        while self.running:
            try:
                # Setup publications for tables
                await self._setup_table_publications()

                await asyncio.sleep(30)  # Check every 30 seconds

            except Exception as e:
                logger.error(f"Error in replication setup loop: {e}")
                await asyncio.sleep(10)
    
    async def _replication_monitoring_loop(self):
        """Monitor replication lag and status"""
        while self.running:
            try:
                await self._update_replication_status()
                await self._check_replication_health()
                
                await asyncio.sleep(10)  # Monitor every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in replication monitoring loop: {e}")
                await asyncio.sleep(5)
    
    async def _subscription_maintenance_loop(self):
        """Maintain and repair subscriptions"""
        while self.running:
            try:
                await self._maintain_subscriptions()
                await self._repair_failed_subscriptions()
                
                await asyncio.sleep(60)  # Maintenance every minute
                
            except Exception as e:
                logger.error(f"Error in subscription maintenance loop: {e}")
                await asyncio.sleep(10)
    
    async def _get_cluster_topology(self) -> List[Dict]:
        """Get cluster topology from gossip protocol or database"""
        try:
            # Prefer gossip protocol for real-time topology
            if self.gossip_protocol:
                members = self.gossip_protocol.get_cluster_members()
                return [{
                    'node_id': m['node_id'],
                    'node_name': m['name'],
                    'host_address': m['host'],
                    'port': m['port'],
                    'status': 'active' if m['state'] == 'alive' else 'inactive'
                } for m in members if m['state'] == 'alive']

            # Fallback to database
            async with self.local_pool.acquire() as conn:
                result = await conn.fetch(
                    "SELECT node_id, host, port, status FROM synapsedb_consensus.cluster_nodes WHERE status = 'active'"
                )

                return [{
                    'node_id': row['node_id'],
                    'node_name': row['node_id'],  # Use node_id as name fallback
                    'host_address': row['host'],
                    'port': row['port'],
                    'status': row['status']
                } for row in result]

        except Exception as e:
            logger.error(f"Failed to get cluster topology: {e}")
            return []
    
    async def _ensure_replication_to_node(self, target_node: Dict):
        """Ensure replication is set up to a target node"""
        target_node_id = target_node['node_id']
        target_node_name = target_node['node_name']
        
        try:
            # Check if subscription already exists
            subscription_name = f"sub_{self.node_name}_to_{target_node_name}"
            
            async with self.local_pool.acquire() as conn:
                sub_exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pglogical.subscription WHERE subscription_name = $1)",
                    subscription_name
                )
                
                if not sub_exists:
                    # Create subscription to target node
                    await self._create_subscription(target_node, subscription_name)
                else:
                    # Update subscription status
                    await self._update_subscription_status(subscription_name, target_node_id)
        
        except Exception as e:
            logger.error(f"Failed to ensure replication to {target_node_name}: {e}")
    
    async def _create_subscription(self, target_node: Dict, subscription_name: str):
        """Create a subscription to a target node"""
        try:
            target_dsn = f"host={target_node['host_address']} port={target_node['port']} dbname={self.db_config['database']} user={self.db_config['user']} password={self.db_config['password']}"
            
            # Use the target node's replication set
            target_replication_set = f"synapsedb_default_{target_node['node_name']}"
            
            async with self.local_pool.acquire() as conn:
                await conn.execute("""
                    SELECT pglogical.create_subscription(
                        subscription_name := $1,
                        provider_dsn := $2,
                        replication_sets := ARRAY[$3],
                        synchronize_structure := false,
                        synchronize_data := false,
                        forward_origins := ARRAY['all']
                    )
                """, subscription_name, target_dsn, target_replication_set)
                
                logger.info(f"Created subscription {subscription_name}")
                
                # Record subscription in tracking table
                await conn.execute("""
                    INSERT INTO synapsedb_replication.replication_links 
                    (source_node_id, target_node_id, subscription_name, subscription_status, created_at)
                    VALUES ($1, $2, $3, 'active', NOW())
                    ON CONFLICT (source_node_id, target_node_id) 
                    DO UPDATE SET 
                        subscription_name = EXCLUDED.subscription_name,
                        subscription_status = 'active',
                        updated_at = NOW()
                """, target_node['node_id'], self.node_id, subscription_name)
                
        except Exception as e:
            logger.error(f"Failed to create subscription {subscription_name}: {e}")
            
            # Mark as failed in tracking table
            try:
                async with self.local_pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO synapsedb_replication.replication_links 
                        (source_node_id, target_node_id, subscription_name, subscription_status, last_error)
                        VALUES ($1, $2, $3, 'failed', $4)
                        ON CONFLICT (source_node_id, target_node_id) 
                        DO UPDATE SET 
                            subscription_status = 'failed',
                            last_error = EXCLUDED.last_error,
                            error_count = error_count + 1,
                            updated_at = NOW()
                    """, target_node['node_id'], self.node_id, subscription_name, str(e))
            except:
                pass
    
    async def _setup_table_publications(self):
        """Set up publications for all replicated tables"""
        try:
            replication_set_name = f"synapsedb_default_{self.node_name}"
            
            async with self.local_pool.acquire() as conn:
                for table_name in self.replicated_tables:
                    # Check if table is already in replication set
                    in_set = await conn.fetchval("""
                        SELECT EXISTS(
                            SELECT 1 FROM pglogical.replication_set_table 
                            WHERE set_name = $1 AND set_reloid = $2::regclass
                        )
                    """, replication_set_name, table_name)
                    
                    if not in_set:
                        try:
                            # Add table to replication set
                            await conn.execute(
                                "SELECT pglogical.replication_set_add_table($1, $2)",
                                replication_set_name, table_name
                            )
                            logger.info(f"Added table {table_name} to replication set {replication_set_name}")
                        except Exception as e:
                            logger.warning(f"Failed to add table {table_name} to replication: {e}")
        
        except Exception as e:
            logger.error(f"Failed to setup table publications: {e}")
    
    async def _update_subscription_status(self, subscription_name: str, target_node_id: str):
        """Update subscription status in tracking table"""
        try:
            async with self.local_pool.acquire() as conn:
                # Get subscription status from pglogical
                sub_status = await conn.fetchrow("""
                    SELECT * FROM pglogical.show_subscription_status($1)
                """, subscription_name)
                
                if sub_status:
                    status = sub_status.get('status', 'unknown')
                    received_lsn = sub_status.get('received_lsn')
                    last_msg_send_time = sub_status.get('last_msg_send_time')
                    
                    # Calculate lag
                    lag_bytes = 0
                    lag_seconds = 0
                    
                    if received_lsn and last_msg_send_time:
                        # Simple lag calculation (would need more sophisticated logic in production)
                        lag_seconds = int((datetime.now() - last_msg_send_time).total_seconds())
                    
                    # Update tracking table
                    await conn.execute("""
                        UPDATE synapsedb_replication.replication_links
                        SET subscription_status = $1,
                            last_sync_time = NOW(),
                            lag_bytes = $2,
                            lag_seconds = $3,
                            updated_at = NOW()
                        WHERE target_node_id = $4 AND subscription_name = $5
                    """, status, lag_bytes, lag_seconds, self.node_id, subscription_name)
        
        except Exception as e:
            logger.warning(f"Failed to update subscription status for {subscription_name}: {e}")
    
    async def _update_replication_status(self):
        """Update overall replication status"""
        try:
            async with self.local_pool.acquire() as conn:
                # Get all active subscriptions
                subscriptions = await conn.fetch("""
                    SELECT subscription_name, provider_dsn, provider_node, subscriber_name
                    FROM pglogical.subscription
                    WHERE enabled = true
                """)
                
                for sub in subscriptions:
                    try:
                        # Get detailed status
                        status = await conn.fetchrow(
                            "SELECT * FROM pglogical.show_subscription_status($1)",
                            sub['subscription_name']
                        )
                        
                        if status:
                            # Parse node information from provider_dsn or subscription name
                            provider_node_name = sub['provider_node'] if 'provider_node' in sub else None
                            
                            if provider_node_name:
                                # Find provider node ID
                                provider_node_id = None
                                for nid, ninfo in self.cluster_nodes.items():
                                    if ninfo.get('name') == provider_node_name:
                                        provider_node_id = nid
                                        break
                                
                                if provider_node_id:
                                    # Calculate replication lag
                                    lag_bytes = 0
                                    lag_seconds = 0
                                    
                                    # Update in database
                                    await conn.execute("""
                                        INSERT INTO synapsedb_replication.replication_links 
                                        (source_node_id, target_node_id, subscription_name, 
                                         subscription_status, lag_bytes, lag_seconds, last_sync_time)
                                        VALUES ($1, $2, $3, $4, $5, $6, NOW())
                                        ON CONFLICT (source_node_id, target_node_id) 
                                        DO UPDATE SET
                                            subscription_status = EXCLUDED.subscription_status,
                                            lag_bytes = EXCLUDED.lag_bytes,
                                            lag_seconds = EXCLUDED.lag_seconds,
                                            last_sync_time = EXCLUDED.last_sync_time,
                                            updated_at = NOW()
                                    """, provider_node_id, self.node_id, sub['subscription_name'],
                                        status.get('status', 'unknown'), lag_bytes, lag_seconds)
                    
                    except Exception as e:
                        logger.warning(f"Failed to update status for subscription {sub['subscription_name']}: {e}")
        
        except Exception as e:
            logger.error(f"Failed to update replication status: {e}")
    
    async def _check_replication_health(self):
        """Check replication health and alert on issues"""
        try:
            async with self.local_pool.acquire() as conn:
                # Check for high lag
                high_lag_links = await conn.fetch("""
                    SELECT source_node_id, target_node_id, subscription_name, lag_seconds
                    FROM synapsedb_replication.replication_links
                    WHERE lag_seconds > $1 AND subscription_status = 'active'
                """, self.replication_lag_threshold)
                
                for link in high_lag_links:
                    logger.warning(
                        f"High replication lag detected: {link['subscription_name']} "
                        f"({link['lag_seconds']} seconds)"
                    )
                
                # Check for failed subscriptions
                failed_links = await conn.fetch("""
                    SELECT source_node_id, target_node_id, subscription_name, last_error
                    FROM synapsedb_replication.replication_links
                    WHERE subscription_status = 'failed'
                """)
                
                for link in failed_links:
                    logger.error(
                        f"Failed replication link: {link['subscription_name']} "
                        f"Error: {link['last_error']}"
                    )
        
        except Exception as e:
            logger.error(f"Failed to check replication health: {e}")
    
    async def _maintain_subscriptions(self):
        """Maintain and optimize subscriptions"""
        try:
            async with self.local_pool.acquire() as conn:
                # Refresh all subscriptions to ensure they're up to date
                subscriptions = await conn.fetch(
                    "SELECT subscription_name FROM pglogical.subscription WHERE enabled = true"
                )
                
                for sub in subscriptions:
                    try:
                        # Refresh subscription (updates table list, etc.)
                        await conn.execute(
                            "SELECT pglogical.alter_subscription_refresh($1)",
                            sub['subscription_name']
                        )
                    except Exception as e:
                        logger.warning(f"Failed to refresh subscription {sub['subscription_name']}: {e}")
        
        except Exception as e:
            logger.error(f"Failed to maintain subscriptions: {e}")
    
    async def _repair_failed_subscriptions(self):
        """Attempt to repair failed subscriptions"""
        try:
            async with self.local_pool.acquire() as conn:
                # Get failed subscriptions
                failed_subs = await conn.fetch("""
                    SELECT DISTINCT subscription_name, source_node_id
                    FROM synapsedb_replication.replication_links
                    WHERE subscription_status = 'failed' 
                    AND error_count < 5  -- Don't retry indefinitely
                    AND updated_at < NOW() - INTERVAL '5 minutes'  -- Wait 5 minutes between retries
                """)
                
                for sub_info in failed_subs:
                    subscription_name = sub_info['subscription_name']
                    source_node_id = sub_info['source_node_id']
                    
                    try:
                        logger.info(f"Attempting to repair failed subscription: {subscription_name}")
                        
                        # Try to re-enable the subscription
                        await conn.execute(
                            "SELECT pglogical.alter_subscription_enable($1, true)",
                            subscription_name
                        )
                        
                        # Update status to indicate repair attempt
                        await conn.execute("""
                            UPDATE synapsedb_replication.replication_links
                            SET subscription_status = 'repairing',
                                updated_at = NOW()
                            WHERE subscription_name = $1
                        """, subscription_name)
                        
                        logger.info(f"Repair initiated for subscription: {subscription_name}")
                        
                    except Exception as e:
                        logger.warning(f"Failed to repair subscription {subscription_name}: {e}")
                        
                        # Increment error count
                        await conn.execute("""
                            UPDATE synapsedb_replication.replication_links
                            SET error_count = error_count + 1,
                                last_error = $1,
                                updated_at = NOW()
                            WHERE subscription_name = $2
                        """, str(e), subscription_name)
        
        except Exception as e:
            logger.error(f"Failed to repair failed subscriptions: {e}")
    
    async def add_table_to_replication(self, table_name: str):
        """Add a table to replication"""
        try:
            async with self.local_pool.acquire() as conn:
                replication_set_name = f"synapsedb_default_{self.node_name}"
                
                # Add table to local replication set
                await conn.execute(
                    "SELECT pglogical.replication_set_add_table($1, $2)",
                    replication_set_name, table_name
                )
                
                logger.info(f"Added table {table_name} to replication")
                self.replicated_tables.add(table_name)
                
                # Refresh all subscriptions to include the new table
                subscriptions = await conn.fetch(
                    "SELECT subscription_name FROM pglogical.subscription WHERE enabled = true"
                )
                
                for sub in subscriptions:
                    try:
                        await conn.execute(
                            "SELECT pglogical.alter_subscription_refresh($1)",
                            sub['subscription_name']
                        )
                    except Exception as e:
                        logger.warning(f"Failed to refresh subscription {sub['subscription_name']} for new table: {e}")
        
        except Exception as e:
            logger.error(f"Failed to add table {table_name} to replication: {e}")
            raise
    
    async def remove_table_from_replication(self, table_name: str):
        """Remove a table from replication"""
        try:
            async with self.local_pool.acquire() as conn:
                replication_set_name = f"synapsedb_default_{self.node_name}"
                
                # Remove table from local replication set
                await conn.execute(
                    "SELECT pglogical.replication_set_remove_table($1, $2)",
                    replication_set_name, table_name
                )
                
                logger.info(f"Removed table {table_name} from replication")
                self.replicated_tables.discard(table_name)
        
        except Exception as e:
            logger.error(f"Failed to remove table {table_name} from replication: {e}")
            raise
    
    async def get_replication_status(self) -> Dict:
        """Get comprehensive replication status"""
        try:
            async with self.local_pool.acquire() as conn:
                # Get subscription status
                subscriptions = await conn.fetch("""
                    SELECT 
                        subscription_name,
                        provider_node, 
                        enabled,
                        (SELECT status FROM pglogical.show_subscription_status(subscription_name)) as status
                    FROM pglogical.subscription
                """)
                
                # Get replication set info
                rep_sets = await conn.fetch("""
                    SELECT set_name, (
                        SELECT COUNT(*) FROM pglogical.replication_set_table 
                        WHERE set_name = rs.set_name
                    ) as table_count
                    FROM pglogical.replication_set rs
                """)
                
                # Get lag information
                lag_info = await conn.fetch("""
                    SELECT source_node_id, target_node_id, lag_seconds, subscription_status
                    FROM synapsedb_replication.replication_links
                    ORDER BY lag_seconds DESC
                """)
                
                return {
                    'node_id': self.node_id,
                    'node_name': self.node_name,
                    'subscriptions': [dict(row) for row in subscriptions],
                    'replication_sets': [dict(row) for row in rep_sets],
                    'lag_info': [dict(row) for row in lag_info],
                    'replicated_tables': list(self.replicated_tables),
                    'timestamp': time.time()
                }
        
        except Exception as e:
            logger.error(f"Failed to get replication status: {e}")
            return {'error': str(e)}

    def _setup_auto_discovery_hooks(self):
        """Setup hooks with gossip protocol for auto-discovery"""
        if not self.gossip_protocol:
            return

        # Store original handle methods
        original_handle_join = self.gossip_protocol._handle_join
        original_handle_leave = self.gossip_protocol._handle_leave
        original_mark_dead = self.gossip_protocol._mark_dead

        # Wrap join handler to trigger replication setup
        async def enhanced_handle_join(message):
            await original_handle_join(message)
            # Trigger replication setup for newly joined node
            if message.payload and 'node_info' in message.payload:
                node_info = message.payload['node_info']
                await self._on_node_discovered({
                    'node_id': node_info['node_id'],
                    'node_name': node_info['name'],
                    'host_address': node_info['host'],
                    'port': node_info['port'],
                    'status': 'active'
                })

        # Wrap leave/dead handlers to clean up replication
        async def enhanced_handle_leave(message):
            await original_handle_leave(message)
            await self._on_node_removed(message.target_id)

        async def enhanced_mark_dead(node_id):
            await original_mark_dead(node_id)
            await self._on_node_removed(node_id)

        # Replace the handlers
        self.gossip_protocol._handle_join = enhanced_handle_join
        self.gossip_protocol._handle_leave = enhanced_handle_leave
        self.gossip_protocol._mark_dead = enhanced_mark_dead

        logger.info("Auto-discovery hooks set up with gossip protocol")

    async def _auto_discovery_loop(self):
        """Auto-discovery loop that continuously discovers and sets up replication"""
        while self.running:
            try:
                if self.gossip_protocol:
                    # Get all alive nodes from gossip protocol
                    cluster_nodes = await self._get_cluster_topology()

                    for node_info in cluster_nodes:
                        if node_info['node_id'] != self.node_id and node_info['status'] == 'active':
                            await self._ensure_replication_to_node(node_info)

                await asyncio.sleep(60)  # Check every minute for auto-discovery

            except Exception as e:
                logger.error(f"Error in auto-discovery loop: {e}")
                await asyncio.sleep(30)

    async def _on_node_discovered(self, node_info: Dict):
        """Called when a new node is discovered via gossip protocol"""
        try:
            logger.info(f"Auto-discovery: New node discovered - {node_info['node_name']}")

            # Ensure replication to the new node
            await self._ensure_replication_to_node(node_info)

            # Add to cluster nodes tracking
            self.cluster_nodes[node_info['node_id']] = {
                'id': node_info['node_id'],
                'host': node_info['host_address'],
                'port': node_info['port'],
                'name': node_info['node_name']
            }

            logger.info(f"Auto-discovery: Set up replication to {node_info['node_name']}")

        except Exception as e:
            logger.error(f"Failed to handle discovered node {node_info.get('node_name', 'unknown')}: {e}")

    async def _on_node_removed(self, node_id: str):
        """Called when a node leaves or dies"""
        try:
            if node_id in self.cluster_nodes:
                node_name = self.cluster_nodes[node_id].get('name', node_id)
                logger.info(f"Auto-discovery: Node removed - {node_name}")

                # Clean up subscriptions to the removed node
                await self._cleanup_subscriptions_to_node(node_id)

                # Remove from tracking
                del self.cluster_nodes[node_id]

        except Exception as e:
            logger.error(f"Failed to handle removed node {node_id}: {e}")

    async def _cleanup_subscriptions_to_node(self, node_id: str):
        """Clean up subscriptions to a removed node"""
        try:
            node_name = self.cluster_nodes.get(node_id, {}).get('name', node_id)
            subscription_name = f"sub_{self.node_name}_to_{node_name}"

            async with self.local_pool.acquire() as conn:
                # Check if subscription exists
                sub_exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pglogical.subscription WHERE subscription_name = $1)",
                    subscription_name
                )

                if sub_exists:
                    # Drop the subscription
                    await conn.execute(
                        "SELECT pglogical.drop_subscription($1)",
                        subscription_name
                    )
                    logger.info(f"Dropped subscription to removed node: {subscription_name}")

                    # Update tracking table
                    await conn.execute("""
                        UPDATE synapsedb_replication.replication_links
                        SET subscription_status = 'dropped',
                            updated_at = NOW()
                        WHERE target_node_id = $1 AND source_node_id = $2
                    """, self.node_id, node_id)

        except Exception as e:
            logger.error(f"Failed to cleanup subscriptions to node {node_id}: {e}")