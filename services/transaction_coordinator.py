"""
SynapseDB Distributed Transaction Coordinator
Implements 2-phase commit protocol for distributed transactions
"""

import asyncio
import asyncpg
import logging
import uuid
import time
import json
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)

class TransactionStatus(Enum):
    PREPARING = "preparing"
    PREPARED = "prepared" 
    COMMITTING = "committing"
    COMMITTED = "committed"
    ABORTING = "aborting"
    ABORTED = "aborted"
    FAILED = "failed"

@dataclass
class DistributedTransaction:
    txn_id: str
    coordinator_node: str
    participant_nodes: List[str]
    operations: List[Dict]
    status: TransactionStatus
    timeout_at: float
    created_at: float
    votes: Dict[str, str] = None  # node_id -> vote (YES/NO)
    
    def __post_init__(self):
        if self.votes is None:
            self.votes = {}

class TransactionCoordinator:
    """2-Phase Commit Transaction Coordinator"""
    
    def __init__(self, node_id: str, db_config: Dict, cluster_nodes: List[Dict]):
        self.node_id = node_id
        self.db_config = db_config
        self.cluster_nodes = {n['id']: n for n in cluster_nodes}
        
        # Database connection
        self.db_pool: Optional[asyncpg.Pool] = None
        
        # Active transactions
        self.active_transactions: Dict[str, DistributedTransaction] = {}
        
        # Running state
        self.running = False
        
        # Transaction timeout (seconds)
        self.transaction_timeout = 30
    
    async def initialize(self):
        """Initialize transaction coordinator"""
        logger.info(f"Initializing transaction coordinator for node {self.node_id}")
        
        self.db_pool = await asyncpg.create_pool(**self.db_config, min_size=3, max_size=15)
        
        # Load active transactions
        await self._load_active_transactions()
        
        logger.info("Transaction coordinator initialized")
    
    async def start(self):
        """Start transaction coordinator"""
        self.running = True
        logger.info("Starting transaction coordinator")
        
        tasks = [
            asyncio.create_task(self._transaction_timeout_loop()),
            asyncio.create_task(self._transaction_recovery_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Transaction coordinator error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop transaction coordinator"""
        self.running = False
        logger.info("Stopping transaction coordinator")
        
        if self.db_pool:
            await self.db_pool.close()
    
    async def begin_distributed_transaction(self, operations: List[Dict], 
                                          participant_nodes: List[str] = None) -> str:
        """Begin a new distributed transaction"""
        try:
            txn_id = str(uuid.uuid4())
            current_time = time.time()
            
            # Determine participant nodes based on operations
            if participant_nodes is None:
                participant_nodes = await self._determine_participant_nodes(operations)
            
            # Create transaction record
            transaction = DistributedTransaction(
                txn_id=txn_id,
                coordinator_node=self.node_id,
                participant_nodes=participant_nodes,
                operations=operations,
                status=TransactionStatus.PREPARING,
                timeout_at=current_time + self.transaction_timeout,
                created_at=current_time
            )
            
            # Store locally
            self.active_transactions[txn_id] = transaction
            
            # Persist to database
            await self._persist_transaction(transaction)
            
            logger.info(f"Started distributed transaction {txn_id} with {len(participant_nodes)} participants")
            
            # Start 2PC protocol
            asyncio.create_task(self._execute_2pc_protocol(txn_id))
            
            return txn_id
            
        except Exception as e:
            logger.error(f"Failed to begin distributed transaction: {e}")
            raise
    
    async def _execute_2pc_protocol(self, txn_id: str):
        """Execute 2-phase commit protocol"""
        try:
            transaction = self.active_transactions[txn_id]
            
            # Phase 1: Prepare
            logger.info(f"Starting Phase 1 (PREPARE) for transaction {txn_id}")
            prepare_success = await self._phase1_prepare(transaction)
            
            if prepare_success:
                # Phase 2: Commit
                logger.info(f"Starting Phase 2 (COMMIT) for transaction {txn_id}")
                transaction.status = TransactionStatus.COMMITTING
                await self._persist_transaction(transaction)
                
                await self._phase2_commit(transaction)
                
                transaction.status = TransactionStatus.COMMITTED
                logger.info(f"Transaction {txn_id} COMMITTED successfully")
            else:
                # Phase 2: Abort
                logger.info(f"Starting Phase 2 (ABORT) for transaction {txn_id}")
                transaction.status = TransactionStatus.ABORTING
                await self._persist_transaction(transaction)
                
                await self._phase2_abort(transaction)
                
                transaction.status = TransactionStatus.ABORTED
                logger.info(f"Transaction {txn_id} ABORTED")
            
            # Final persistence
            await self._persist_transaction(transaction)
            
        except Exception as e:
            logger.error(f"Error in 2PC protocol for transaction {txn_id}: {e}")
            
            # Mark as failed and try to abort
            if txn_id in self.active_transactions:
                transaction = self.active_transactions[txn_id]
                transaction.status = TransactionStatus.FAILED
                await self._persist_transaction(transaction)
                
                # Try to abort on all participants
                await self._phase2_abort(transaction)
    
    async def _phase1_prepare(self, transaction: DistributedTransaction) -> bool:
        """Phase 1: Send PREPARE to all participants"""
        try:
            prepare_tasks = []
            
            for participant_node in transaction.participant_nodes:
                if participant_node in self.cluster_nodes:
                    task = asyncio.create_task(
                        self._send_prepare_request(participant_node, transaction)
                    )
                    prepare_tasks.append(task)
            
            # Wait for all prepare responses (with timeout)
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*prepare_tasks, return_exceptions=True),
                    timeout=self.transaction_timeout / 2
                )
                
                # Check if all participants voted YES
                yes_votes = sum(1 for r in results if r == 'YES')
                required_votes = len(transaction.participant_nodes)
                
                if yes_votes == required_votes:
                    transaction.status = TransactionStatus.PREPARED
                    return True
                else:
                    logger.warning(f"Transaction {transaction.txn_id} prepare failed: {yes_votes}/{required_votes} YES votes")
                    return False
                    
            except asyncio.TimeoutError:
                logger.warning(f"Transaction {transaction.txn_id} prepare phase timed out")
                return False
                
        except Exception as e:
            logger.error(f"Error in prepare phase for transaction {transaction.txn_id}: {e}")
            return False
    
    async def _send_prepare_request(self, participant_node: str, transaction: DistributedTransaction) -> str:
        """Send PREPARE request to a participant node"""
        try:
            node_info = self.cluster_nodes[participant_node]
            
            # Get operations for this participant
            participant_operations = [
                op for op in transaction.operations 
                if self._operation_affects_node(op, participant_node)
            ]
            
            prepare_request = {
                'txn_id': transaction.txn_id,
                'coordinator_node': self.node_id,
                'operations': participant_operations,
                'timeout': transaction.timeout_at
            }
            
            # Send HTTP request (in production would use more reliable transport)
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = f"http://{node_info['host']}:{node_info.get('tx_port', 8081)}/2pc/prepare"
                async with session.post(url, json=prepare_request) as response:
                    if response.status == 200:
                        result = await response.json()
                        vote = result.get('vote', 'NO')
                        
                        # Record vote
                        transaction.votes[participant_node] = vote
                        return vote
                    else:
                        logger.warning(f"Prepare request to {participant_node} failed with status {response.status}")
                        return 'NO'
                        
        except Exception as e:
            logger.error(f"Failed to send prepare request to {participant_node}: {e}")
            return 'NO'
    
    async def _phase2_commit(self, transaction: DistributedTransaction):
        """Phase 2: Send COMMIT to all participants"""
        try:
            commit_tasks = []
            
            for participant_node in transaction.participant_nodes:
                if participant_node in self.cluster_nodes:
                    task = asyncio.create_task(
                        self._send_commit_request(participant_node, transaction)
                    )
                    commit_tasks.append(task)
            
            # Send commit requests (don't wait for all - best effort)
            results = await asyncio.gather(*commit_tasks, return_exceptions=True)
            
            # Log any failed commits
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    participant = transaction.participant_nodes[i]
                    logger.error(f"Failed to send commit to {participant}: {result}")
                    
        except Exception as e:
            logger.error(f"Error in commit phase for transaction {transaction.txn_id}: {e}")
    
    async def _send_commit_request(self, participant_node: str, transaction: DistributedTransaction):
        """Send COMMIT request to a participant node"""
        try:
            node_info = self.cluster_nodes[participant_node]
            
            commit_request = {
                'txn_id': transaction.txn_id,
                'coordinator_node': self.node_id,
                'decision': 'COMMIT'
            }
            
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = f"http://{node_info['host']}:{node_info.get('tx_port', 8081)}/2pc/commit"
                async with session.post(url, json=commit_request) as response:
                    if response.status != 200:
                        logger.warning(f"Commit request to {participant_node} failed with status {response.status}")
                        
        except Exception as e:
            logger.error(f"Failed to send commit request to {participant_node}: {e}")
    
    async def _phase2_abort(self, transaction: DistributedTransaction):
        """Phase 2: Send ABORT to all participants"""
        try:
            abort_tasks = []
            
            for participant_node in transaction.participant_nodes:
                if participant_node in self.cluster_nodes:
                    task = asyncio.create_task(
                        self._send_abort_request(participant_node, transaction)
                    )
                    abort_tasks.append(task)
            
            # Send abort requests
            await asyncio.gather(*abort_tasks, return_exceptions=True)
                    
        except Exception as e:
            logger.error(f"Error in abort phase for transaction {transaction.txn_id}: {e}")
    
    async def _send_abort_request(self, participant_node: str, transaction: DistributedTransaction):
        """Send ABORT request to a participant node"""
        try:
            node_info = self.cluster_nodes[participant_node]
            
            abort_request = {
                'txn_id': transaction.txn_id,
                'coordinator_node': self.node_id,
                'decision': 'ABORT'
            }
            
            import aiohttp
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = f"http://{node_info['host']}:{node_info.get('tx_port', 8081)}/2pc/abort"
                async with session.post(url, json=abort_request) as response:
                    if response.status != 200:
                        logger.warning(f"Abort request to {participant_node} failed with status {response.status}")
                        
        except Exception as e:
            logger.error(f"Failed to send abort request to {participant_node}: {e}")
    
    async def _determine_participant_nodes(self, operations: List[Dict]) -> List[str]:
        """Determine which nodes need to participate in the transaction"""
        participant_nodes = set()
        
        for operation in operations:
            # Extract table and key information
            table_name = operation.get('table')
            key_value = operation.get('key')
            
            if table_name and key_value:
                # Use sharding manager to find nodes (simplified)
                # In practice, would integrate with actual sharding manager
                nodes = list(self.cluster_nodes.keys())[:2]  # Simplified - take first 2 nodes
                participant_nodes.update(nodes)
        
        return list(participant_nodes)
    
    def _operation_affects_node(self, operation: Dict, node_id: str) -> bool:
        """Check if an operation affects a specific node"""
        # Simplified check - in practice would use actual shard routing
        return True  # For now, assume all operations affect all nodes
    
    async def _persist_transaction(self, transaction: DistributedTransaction):
        """Persist transaction state to database"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synapsedb_consensus.distributed_transactions
                    (txn_id, coordinator_node, participant_nodes, operations, status, votes, timeout_at, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7), to_timestamp($8), NOW())
                    ON CONFLICT (txn_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        votes = EXCLUDED.votes,
                        updated_at = NOW()
                """, transaction.txn_id, transaction.coordinator_node, 
                    transaction.participant_nodes, json.dumps(transaction.operations),
                    transaction.status.value, json.dumps(transaction.votes),
                    transaction.timeout_at, transaction.created_at)
                    
        except Exception as e:
            logger.error(f"Failed to persist transaction {transaction.txn_id}: {e}")
    
    async def _load_active_transactions(self):
        """Load active transactions from database"""
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT txn_id, coordinator_node, participant_nodes, operations, 
                           status, votes, timeout_at, created_at
                    FROM synapsedb_consensus.distributed_transactions
                    WHERE status IN ('preparing', 'prepared', 'committing', 'aborting')
                    AND coordinator_node = $1
                """, self.node_id)
                
                for row in rows:
                    transaction = DistributedTransaction(
                        txn_id=row['txn_id'],
                        coordinator_node=row['coordinator_node'],
                        participant_nodes=row['participant_nodes'],
                        operations=json.loads(row['operations']),
                        status=TransactionStatus(row['status']),
                        timeout_at=row['timeout_at'].timestamp(),
                        created_at=row['created_at'].timestamp(),
                        votes=json.loads(row['votes']) if row['votes'] else {}
                    )
                    
                    self.active_transactions[transaction.txn_id] = transaction
                
                logger.info(f"Loaded {len(self.active_transactions)} active transactions")
                
        except Exception as e:
            logger.error(f"Failed to load active transactions: {e}")
    
    async def _transaction_timeout_loop(self):
        """Handle transaction timeouts"""
        while self.running:
            try:
                current_time = time.time()
                timed_out_txns = []
                
                for txn_id, transaction in self.active_transactions.items():
                    if current_time > transaction.timeout_at and transaction.status not in [
                        TransactionStatus.COMMITTED, TransactionStatus.ABORTED, TransactionStatus.FAILED
                    ]:
                        timed_out_txns.append(txn_id)
                
                # Abort timed out transactions
                for txn_id in timed_out_txns:
                    logger.warning(f"Transaction {txn_id} timed out, aborting")
                    transaction = self.active_transactions[txn_id]
                    transaction.status = TransactionStatus.ABORTING
                    await self._phase2_abort(transaction)
                    transaction.status = TransactionStatus.ABORTED
                    await self._persist_transaction(transaction)
                
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error in timeout loop: {e}")
                await asyncio.sleep(5)
    
    async def _transaction_recovery_loop(self):
        """Recover from coordinator failures"""
        while self.running:
            try:
                # Look for transactions that need recovery
                # This would involve checking for transactions from failed coordinators
                # and either taking over or aborting them
                
                await asyncio.sleep(60)  # Recovery check every minute
                
            except Exception as e:
                logger.error(f"Error in recovery loop: {e}")
                await asyncio.sleep(30)
    
    async def get_transaction_status(self, txn_id: str) -> Optional[Dict]:
        """Get status of a transaction"""
        if txn_id in self.active_transactions:
            transaction = self.active_transactions[txn_id]
            return {
                'txn_id': transaction.txn_id,
                'status': transaction.status.value,
                'coordinator_node': transaction.coordinator_node,
                'participant_nodes': transaction.participant_nodes,
                'votes': transaction.votes,
                'created_at': transaction.created_at,
                'timeout_at': transaction.timeout_at
            }
        
        return None