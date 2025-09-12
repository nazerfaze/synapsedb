"""
SynapseDB Vector Clock System
Provides causal consistency and conflict resolution for distributed operations
"""

import json
import time
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class ConflictResolutionStrategy(Enum):
    LAST_WRITE_WINS = "last_write_wins"
    MERGE = "merge"
    CUSTOM = "custom"

@dataclass
class VectorClock:
    """Vector clock for tracking causal ordering of events"""
    clocks: Dict[str, int]
    
    def __post_init__(self):
        if self.clocks is None:
            self.clocks = {}
    
    def increment(self, node_id: str) -> 'VectorClock':
        """Increment clock for a specific node"""
        new_clocks = self.clocks.copy()
        new_clocks[node_id] = new_clocks.get(node_id, 0) + 1
        return VectorClock(new_clocks)
    
    def update(self, other: 'VectorClock') -> 'VectorClock':
        """Update this vector clock based on another (for message reception)"""
        new_clocks = {}
        all_nodes = set(self.clocks.keys()) | set(other.clocks.keys())
        
        for node_id in all_nodes:
            self_clock = self.clocks.get(node_id, 0)
            other_clock = other.clocks.get(node_id, 0)
            new_clocks[node_id] = max(self_clock, other_clock)
        
        return VectorClock(new_clocks)
    
    def happens_before(self, other: 'VectorClock') -> bool:
        """Check if this event happens before another event"""
        if not other.clocks:
            return False
        
        strictly_less = False
        for node_id, other_clock in other.clocks.items():
            self_clock = self.clocks.get(node_id, 0)
            if self_clock > other_clock:
                return False
            elif self_clock < other_clock:
                strictly_less = True
        
        return strictly_less
    
    def concurrent_with(self, other: 'VectorClock') -> bool:
        """Check if this event is concurrent with another event"""
        return not (self.happens_before(other) or other.happens_before(self))
    
    def compare(self, other: 'VectorClock') -> str:
        """Compare two vector clocks and return relationship"""
        if self.happens_before(other):
            return "before"
        elif other.happens_before(self):
            return "after"
        elif self == other:
            return "equal"
        else:
            return "concurrent"
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary for serialization"""
        return self.clocks.copy()
    
    @classmethod
    def from_dict(cls, data: Dict[str, int]) -> 'VectorClock':
        """Create from dictionary"""
        return cls(data.copy())
    
    def __eq__(self, other: 'VectorClock') -> bool:
        if not isinstance(other, VectorClock):
            return False
        return self.clocks == other.clocks
    
    def __str__(self) -> str:
        return f"VectorClock({self.clocks})"

@dataclass
class CausalEvent:
    """An event with causal ordering information"""
    event_id: str
    node_id: str
    timestamp: float
    vector_clock: VectorClock
    event_type: str
    data: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        return {
            'event_id': self.event_id,
            'node_id': self.node_id,
            'timestamp': self.timestamp,
            'vector_clock': self.vector_clock.to_dict(),
            'event_type': self.event_type,
            'data': self.data
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CausalEvent':
        """Deserialize from dictionary"""
        return cls(
            event_id=data['event_id'],
            node_id=data['node_id'],
            timestamp=data['timestamp'],
            vector_clock=VectorClock.from_dict(data['vector_clock']),
            event_type=data['event_type'],
            data=data['data']
        )

class ConflictResolver:
    """Handles conflict resolution between concurrent events"""
    
    def __init__(self, strategy: ConflictResolutionStrategy = ConflictResolutionStrategy.LAST_WRITE_WINS):
        self.strategy = strategy
    
    def resolve_conflict(self, events: List[CausalEvent]) -> CausalEvent:
        """Resolve conflicts between concurrent events"""
        if len(events) <= 1:
            return events[0] if events else None
        
        # Check if all events are truly concurrent
        concurrent_events = []
        for event in events:
            is_concurrent = True
            for other_event in events:
                if event != other_event:
                    if event.vector_clock.happens_before(other_event.vector_clock):
                        is_concurrent = False
                        break
            if is_concurrent:
                concurrent_events.append(event)
        
        if len(concurrent_events) <= 1:
            # No actual conflicts, return the latest causally
            return self._find_latest_causal(events)
        
        # Resolve based on strategy
        if self.strategy == ConflictResolutionStrategy.LAST_WRITE_WINS:
            return self._last_write_wins(concurrent_events)
        elif self.strategy == ConflictResolutionStrategy.MERGE:
            return self._merge_events(concurrent_events)
        else:
            return self._custom_resolution(concurrent_events)
    
    def _find_latest_causal(self, events: List[CausalEvent]) -> CausalEvent:
        """Find the event that comes latest in causal order"""
        if not events:
            return None
        
        latest = events[0]
        for event in events[1:]:
            if latest.vector_clock.happens_before(event.vector_clock):
                latest = event
        
        return latest
    
    def _last_write_wins(self, events: List[CausalEvent]) -> CausalEvent:
        """Last write wins based on timestamp"""
        return max(events, key=lambda e: e.timestamp)
    
    def _merge_events(self, events: List[CausalEvent]) -> CausalEvent:
        """Merge concurrent events (application-specific)"""
        # For simplicity, merge data dictionaries
        # In practice, this would be application-specific
        base_event = events[0]
        merged_data = {}
        
        for event in events:
            merged_data.update(event.data)
        
        # Create new vector clock that dominates all input clocks
        merged_clock = events[0].vector_clock
        for event in events[1:]:
            merged_clock = merged_clock.update(event.vector_clock)
        
        return CausalEvent(
            event_id=f"merged_{base_event.event_id}",
            node_id=base_event.node_id,
            timestamp=max(e.timestamp for e in events),
            vector_clock=merged_clock,
            event_type="merged",
            data=merged_data
        )
    
    def _custom_resolution(self, events: List[CausalEvent]) -> CausalEvent:
        """Custom conflict resolution logic"""
        # Placeholder for custom resolution
        return self._last_write_wins(events)

class CausalConsistencyManager:
    """Manages causal consistency across distributed nodes"""
    
    def __init__(self, node_id: str, cluster_nodes: List[str]):
        self.node_id = node_id
        self.cluster_nodes = cluster_nodes
        self.vector_clock = VectorClock({node: 0 for node in cluster_nodes})
        self.event_buffer: Dict[str, List[CausalEvent]] = {}  # key -> events
        self.delivered_events: Dict[str, CausalEvent] = {}
        self.conflict_resolver = ConflictResolver()
        
        # Causal ordering buffer
        self.pending_events: List[CausalEvent] = []
        self.max_buffer_size = 10000
    
    def create_event(self, event_type: str, key: str, data: Dict[str, Any]) -> CausalEvent:
        """Create a new causal event"""
        import uuid
        
        # Increment our own clock
        self.vector_clock = self.vector_clock.increment(self.node_id)
        
        event = CausalEvent(
            event_id=str(uuid.uuid4()),
            node_id=self.node_id,
            timestamp=time.time(),
            vector_clock=self.vector_clock,
            event_type=event_type,
            data=data
        )
        
        logger.debug(f"Created event {event.event_id} with clock {event.vector_clock}")
        return event
    
    def receive_event(self, event: CausalEvent) -> bool:
        """Receive an event from another node"""
        try:
            # Update our vector clock
            self.vector_clock = self.vector_clock.update(event.vector_clock)
            
            # Add to pending events for causal ordering
            self.pending_events.append(event)
            
            # Try to deliver causally ordered events
            self._deliver_ready_events()
            
            logger.debug(f"Received event {event.event_id} from node {event.node_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error receiving event {event.event_id}: {e}")
            return False
    
    def _deliver_ready_events(self):
        """Deliver events that are ready for causal delivery"""
        delivered = []
        
        for event in self.pending_events:
            if self._can_deliver(event):
                self._deliver_event(event)
                delivered.append(event)
        
        # Remove delivered events
        for event in delivered:
            self.pending_events.remove(event)
        
        # Prevent buffer overflow
        if len(self.pending_events) > self.max_buffer_size:
            # Sort by timestamp and deliver oldest events
            self.pending_events.sort(key=lambda e: e.timestamp)
            overflow_count = len(self.pending_events) - self.max_buffer_size
            for i in range(overflow_count):
                event = self.pending_events.pop(0)
                logger.warning(f"Force delivering event {event.event_id} due to buffer overflow")
                self._deliver_event(event)
    
    def _can_deliver(self, event: CausalEvent) -> bool:
        """Check if an event can be delivered (all causal dependencies satisfied)"""
        # An event can be delivered if all events it causally depends on have been delivered
        for node_id, clock_value in event.vector_clock.clocks.items():
            if node_id == event.node_id:
                # For the originating node, we need exactly the next sequence number
                our_clock = self.vector_clock.clocks.get(node_id, 0)
                if clock_value != our_clock:
                    return False
            else:
                # For other nodes, we need at least this clock value
                our_clock = self.vector_clock.clocks.get(node_id, 0)
                if clock_value > our_clock:
                    return False
        
        return True
    
    def _deliver_event(self, event: CausalEvent):
        """Deliver an event to the application"""
        key = self._extract_key_from_event(event)
        
        # Add to buffer for conflict resolution
        if key not in self.event_buffer:
            self.event_buffer[key] = []
        
        self.event_buffer[key].append(event)
        
        # Check for conflicts and resolve
        resolved_event = self._resolve_conflicts_for_key(key)
        if resolved_event:
            self.delivered_events[key] = resolved_event
            logger.debug(f"Delivered resolved event for key {key}: {resolved_event.event_id}")
    
    def _extract_key_from_event(self, event: CausalEvent) -> str:
        """Extract the key that this event affects"""
        # Default implementation - use table.key format
        table = event.data.get('table', 'unknown')
        key = event.data.get('key', event.event_id)
        return f"{table}.{key}"
    
    def _resolve_conflicts_for_key(self, key: str) -> Optional[CausalEvent]:
        """Resolve conflicts for events affecting the same key"""
        events = self.event_buffer[key]
        if not events:
            return None
        
        # Find concurrent events
        concurrent_groups = self._group_concurrent_events(events)
        
        resolved_events = []
        for group in concurrent_groups:
            if len(group) > 1:
                # Conflict detected, resolve
                resolved = self.conflict_resolver.resolve_conflict(group)
                if resolved:
                    resolved_events.append(resolved)
            else:
                resolved_events.extend(group)
        
        # Return the latest resolved event
        if resolved_events:
            return max(resolved_events, key=lambda e: e.timestamp)
        
        return None
    
    def _group_concurrent_events(self, events: List[CausalEvent]) -> List[List[CausalEvent]]:
        """Group events into concurrent sets"""
        if not events:
            return []
        
        groups = []
        processed = set()
        
        for event in events:
            if event.event_id in processed:
                continue
            
            # Find all events concurrent with this one
            concurrent_group = [event]
            processed.add(event.event_id)
            
            for other_event in events:
                if (other_event.event_id not in processed and 
                    event.vector_clock.concurrent_with(other_event.vector_clock)):
                    concurrent_group.append(other_event)
                    processed.add(other_event.event_id)
            
            groups.append(concurrent_group)
        
        return groups
    
    def get_current_clock(self) -> VectorClock:
        """Get current vector clock"""
        return self.vector_clock
    
    def get_delivered_event(self, key: str) -> Optional[CausalEvent]:
        """Get the latest delivered event for a key"""
        return self.delivered_events.get(key)
    
    def get_pending_events_count(self) -> int:
        """Get number of pending events awaiting causal delivery"""
        return len(self.pending_events)
    
    def cleanup_old_events(self, max_age_seconds: int = 3600):
        """Clean up old events to prevent memory leaks"""
        current_time = time.time()
        cutoff_time = current_time - max_age_seconds
        
        # Clean up delivered events
        keys_to_remove = []
        for key, event in self.delivered_events.items():
            if event.timestamp < cutoff_time:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.delivered_events[key]
            if key in self.event_buffer:
                del self.event_buffer[key]
        
        # Clean up pending events
        self.pending_events = [
            event for event in self.pending_events 
            if event.timestamp >= cutoff_time
        ]
        
        logger.debug(f"Cleaned up {len(keys_to_remove)} old events")