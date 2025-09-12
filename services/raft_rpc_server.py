"""
SynapseDB Raft RPC Server
Handles Raft RPC requests (RequestVote, AppendEntries, etc.)
"""

import asyncio
import logging
from typing import Dict, Any
from aiohttp import web, WSMsgType
import json
import time
from datetime import datetime, timedelta

from .raft_consensus import RaftNode, VoteRequest, VoteResponse, AppendEntriesRequest, AppendEntriesResponse, LogEntry, EntryType

logger = logging.getLogger(__name__)

class RaftRPCServer:
    """HTTP server for Raft RPC requests"""
    
    def __init__(self, raft_node: RaftNode, host: str = '0.0.0.0', port: int = 8080):
        self.raft_node = raft_node
        self.host = host
        self.port = port
        self.app = web.Application()
        self.runner = None
        
        # Leadership lease tracking
        self.leadership_leases: Dict[str, datetime] = {}
        
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup HTTP routes for Raft RPC"""
        # Raft RPC endpoints
        self.app.router.add_post('/raft/vote', self._handle_request_vote)
        self.app.router.add_post('/raft/append', self._handle_append_entries)
        self.app.router.add_post('/raft/lease', self._handle_lease_renewal)
        
        # Status and management endpoints
        self.app.router.add_get('/raft/status', self._handle_status)
        self.app.router.add_get('/raft/leader', self._handle_get_leader)
        self.app.router.add_post('/raft/command', self._handle_submit_command)
        
        # Health check
        self.app.router.add_get('/health', self._handle_health)
    
    async def start(self):
        """Start the RPC server"""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()
        
        logger.info(f"Raft RPC server started on {self.host}:{self.port}")
    
    async def stop(self):
        """Stop the RPC server"""
        if self.runner:
            await self.runner.cleanup()
        logger.info("Raft RPC server stopped")
    
    async def _handle_request_vote(self, request):
        """Handle RequestVote RPC"""
        try:
            data = await request.json()
            vote_request = VoteRequest(**data)
            
            logger.debug(f"Received vote request from {vote_request.candidate_id} for term {vote_request.term}")
            
            async with self.raft_node.state_lock:
                vote_granted = False
                current_term = self.raft_node.state.current_term
                
                # Check if request term is valid
                if vote_request.term < current_term:
                    logger.debug(f"Rejecting vote - stale term {vote_request.term} < {current_term}")
                elif vote_request.term > current_term:
                    # Update to newer term
                    await self.raft_node._become_follower(vote_request.term)
                    current_term = vote_request.term
                
                # Check if we can vote for this candidate
                can_vote = (
                    self.raft_node.state.voted_for is None or 
                    self.raft_node.state.voted_for == vote_request.candidate_id
                )
                
                # Check if candidate's log is at least as up-to-date as ours
                last_log_index = len(self.raft_node.log)
                last_log_term = self.raft_node.log[-1].term if self.raft_node.log else 0
                
                log_up_to_date = (
                    vote_request.last_log_term > last_log_term or
                    (vote_request.last_log_term == last_log_term and 
                     vote_request.last_log_index >= last_log_index)
                )
                
                if can_vote and log_up_to_date and vote_request.term == current_term:
                    vote_granted = True
                    self.raft_node.state.voted_for = vote_request.candidate_id
                    self.raft_node._reset_election_timer()
                    await self.raft_node._persist_state()
                    logger.info(f"Granted vote to {vote_request.candidate_id} for term {vote_request.term}")
                else:
                    logger.debug(f"Denied vote to {vote_request.candidate_id}: can_vote={can_vote}, log_up_to_date={log_up_to_date}")
                
                vote_response = VoteResponse(
                    term=current_term,
                    vote_granted=vote_granted
                )
                
                return web.json_response(vote_response.__dict__)
                
        except Exception as e:
            logger.error(f"Error handling vote request: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def _handle_append_entries(self, request):
        """Handle AppendEntries RPC"""
        try:
            data = await request.json()
            
            # Convert entries back to LogEntry objects
            entries_data = data.pop('entries', [])
            entries = [LogEntry(**entry) for entry in entries_data]
            
            append_request = AppendEntriesRequest(**data, entries=entries)
            
            logger.debug(f"Received append entries from {append_request.leader_id}, "
                        f"term {append_request.term}, {len(entries)} entries")
            
            async with self.raft_node.state_lock:
                success = False
                current_term = self.raft_node.state.current_term
                
                # Check if request term is valid
                if append_request.term < current_term:
                    logger.debug(f"Rejecting append entries - stale term {append_request.term} < {current_term}")
                elif append_request.term >= current_term:
                    # Update to newer term and become follower
                    if append_request.term > current_term:
                        await self.raft_node._become_follower(append_request.term)
                    elif self.raft_node.state.role != self.raft_node.state.role.FOLLOWER:
                        await self.raft_node._become_follower(append_request.term)
                    
                    current_term = self.raft_node.state.current_term
                    self.raft_node.state.leader_id = append_request.leader_id
                    self.raft_node._reset_election_timer()
                    
                    # Check log consistency
                    log_consistent = True
                    if append_request.prev_log_index > 0:
                        if (append_request.prev_log_index > len(self.raft_node.log) or
                            (append_request.prev_log_index <= len(self.raft_node.log) and
                             self.raft_node.log[append_request.prev_log_index - 1].term != append_request.prev_log_term)):
                            log_consistent = False
                            logger.debug(f"Log inconsistency at index {append_request.prev_log_index}")
                    
                    if log_consistent:
                        success = True
                        
                        # Append new entries
                        if entries:
                            # Remove conflicting entries
                            start_index = append_request.prev_log_index
                            if start_index < len(self.raft_node.log):
                                # Truncate log from start_index
                                async with self.raft_node.log_lock:
                                    self.raft_node.log = self.raft_node.log[:start_index]
                            
                            # Append new entries
                            async with self.raft_node.log_lock:
                                for entry in entries:
                                    entry.index = len(self.raft_node.log) + 1
                                    self.raft_node.log.append(entry)
                                    await self.raft_node._persist_log_entry(entry)
                        
                        # Update commit index
                        if append_request.leader_commit > self.raft_node.state.commit_index:
                            self.raft_node.state.commit_index = min(
                                append_request.leader_commit,
                                len(self.raft_node.log)
                            )
                        
                        await self.raft_node._persist_state()
                
                append_response = AppendEntriesResponse(
                    term=current_term,
                    success=success,
                    last_log_index=len(self.raft_node.log)
                )
                
                return web.json_response(append_response.__dict__)
                
        except Exception as e:
            logger.error(f"Error handling append entries: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def _handle_lease_renewal(self, request):
        """Handle leadership lease renewal request"""
        try:
            data = await request.json()
            leader_id = data.get('leader_id')
            term = data.get('term')
            lease_duration = data.get('lease_duration', 10.0)
            
            async with self.raft_node.state_lock:
                current_term = self.raft_node.state.current_term
                
                # Grant lease if leader is legitimate
                if (term >= current_term and 
                    (self.raft_node.state.leader_id == leader_id or 
                     self.raft_node.state.role == self.raft_node.state.role.FOLLOWER)):
                    
                    # Update lease
                    lease_expiry = datetime.now() + timedelta(seconds=lease_duration)
                    self.leadership_leases[leader_id] = lease_expiry
                    
                    # Update our view of the leader
                    if term > current_term:
                        await self.raft_node._become_follower(term)
                    
                    self.raft_node.state.leader_id = leader_id
                    self.raft_node._reset_election_timer()
                    
                    logger.debug(f"Granted leadership lease to {leader_id} until {lease_expiry}")
                    return web.json_response({'granted': True})
                else:
                    logger.debug(f"Denied leadership lease to {leader_id} (term {term} vs {current_term})")
                    return web.json_response({'granted': False})
                    
        except Exception as e:
            logger.error(f"Error handling lease renewal: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def _handle_status(self, request):
        """Handle status request"""
        try:
            async with self.raft_node.state_lock:
                status = {
                    'node_id': self.raft_node.node_id,
                    'role': self.raft_node.state.role.value,
                    'current_term': self.raft_node.state.current_term,
                    'leader_id': self.raft_node.state.leader_id,
                    'voted_for': self.raft_node.state.voted_for,
                    'log_length': len(self.raft_node.log),
                    'commit_index': self.raft_node.state.commit_index,
                    'last_applied': self.raft_node.state.last_applied,
                    'cluster_size': len(self.raft_node.cluster_nodes),
                    'timestamp': time.time()
                }
                
                # Add leader-specific status
                if self.raft_node.state.role == self.raft_node.state.role.LEADER:
                    status['next_index'] = self.raft_node.state.next_index
                    status['match_index'] = self.raft_node.state.match_index
                
                return web.json_response(status)
                
        except Exception as e:
            logger.error(f"Error handling status request: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def _handle_get_leader(self, request):
        """Handle get leader request"""
        try:
            leader_info = {
                'leader_id': self.raft_node.state.leader_id,
                'term': self.raft_node.state.current_term,
                'is_leader': self.raft_node.is_leader()
            }
            
            if self.raft_node.state.leader_id and self.raft_node.state.leader_id in self.raft_node.cluster_nodes:
                leader_node = self.raft_node.cluster_nodes[self.raft_node.state.leader_id]
                leader_info['leader_host'] = leader_node.get('host')
                leader_info['leader_port'] = leader_node.get('port')
            
            return web.json_response(leader_info)
            
        except Exception as e:
            logger.error(f"Error handling get leader request: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def _handle_submit_command(self, request):
        """Handle command submission"""
        try:
            data = await request.json()
            
            if not self.raft_node.is_leader():
                # Return leader information for redirect
                leader_info = {
                    'error': 'not_leader',
                    'leader_id': self.raft_node.state.leader_id,
                    'term': self.raft_node.state.current_term
                }
                
                if self.raft_node.state.leader_id and self.raft_node.state.leader_id in self.raft_node.cluster_nodes:
                    leader_node = self.raft_node.cluster_nodes[self.raft_node.state.leader_id]
                    leader_info['leader_host'] = leader_node.get('host')
                    leader_info['leader_port'] = leader_node.get('raft_port', 8080)
                
                return web.json_response(leader_info, status=400)
            
            # Submit command to Raft
            success = await self.raft_node.submit_command(data)
            
            if success:
                return web.json_response({
                    'success': True,
                    'leader_id': self.raft_node.node_id,
                    'term': self.raft_node.state.current_term
                })
            else:
                return web.json_response({'error': 'command_submission_failed'}, status=500)
                
        except Exception as e:
            logger.error(f"Error handling command submission: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def _handle_health(self, request):
        """Handle health check"""
        try:
            health_status = {
                'status': 'healthy',
                'node_id': self.raft_node.node_id,
                'role': self.raft_node.state.role.value,
                'term': self.raft_node.state.current_term,
                'has_leader': self.raft_node.state.leader_id is not None,
                'timestamp': time.time()
            }
            
            return web.json_response(health_status)
            
        except Exception as e:
            logger.error(f"Error handling health check: {e}")
            return web.json_response({'error': str(e)}, status=500)