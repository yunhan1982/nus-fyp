import json
import time
import random
import hashlib
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from collections import defaultdict
import numpy as np
from .optimization_config import OptimizationConfig

@dataclass
class AttributeStats:
    """Statistics for a single attribute in a temporal region."""
    distinct_count: int = 0
    total_count: int = 0
    value_frequencies: Dict[Any, int] = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)
    
    @property
    def selectivity(self) -> float:
        """Calculate selectivity (0 = not selective, 1 = highly selective)."""
        if self.total_count == 0:
            return 0.0
        return self.distinct_count / self.total_count
    
    def update_with_value(self, value: Any, weight: float = 1.0):
        """Update statistics with a new value."""
        if value not in self.value_frequencies:
            self.distinct_count += 1
            self.value_frequencies[value] = 0
        
        self.value_frequencies[value] += weight
        self.total_count += weight
        self.last_updated = time.time()
    
    def estimate_selectivity_for_value(self, value: Any) -> float:
        """Estimate selectivity for a specific value."""
        if self.total_count == 0:
            return 1.0
        
        frequency = self.value_frequencies.get(value, 0)
        return 1.0 - (frequency / self.total_count)

@dataclass
class TemporalRegion:
    """Represents a region in bitemporal space with statistics."""
    vt_min: float
    vt_max: float
    tt_min: float
    tt_max: float
    attribute_stats: Dict[str, AttributeStats] = field(default_factory=dict)
    sample_count: int = 0
    last_updated: float = field(default_factory=time.time)
    
    def contains_point(self, vt: float, tt: float) -> bool:
        """Check if a bitemporal point is within this region."""
        return (self.vt_min <= vt <= self.vt_max and 
                self.tt_min <= tt <= self.tt_max)
    
    def get_attribute_stats(self, attribute: str) -> AttributeStats:
        """Get or create attribute statistics for this region."""
        if attribute not in self.attribute_stats:
            self.attribute_stats[attribute] = AttributeStats()
        return self.attribute_stats[attribute]
    
    def update_with_sample(self, vt: float, tt: float, data: Dict[str, Any], weight: float = 1.0):
        """Update region statistics with a sample."""
        if not self.contains_point(vt, tt):
            return
        
        for attr, value in data.items():
            if attr != 'payload':  # Skip the full payload
                stats = self.get_attribute_stats(attr)
                stats.update_with_value(value, weight)
        
        self.sample_count += 1
        self.last_updated = time.time()

class BitemporalStatisticsCollector:
    """Base class for bitemporal statistics collection."""
    
    def __init__(self, config: OptimizationConfig):
        self.config = config
        self.regions: List[TemporalRegion] = []
        self.samples: List[Tuple[float, float, Dict[str, Any]]] = []  # (vt, tt, data)
        self.last_statistics_update = time.time()
        self.total_records_processed = 0
        self.global_attribute_stats: Dict[str, AttributeStats] = {}
        
        # Initialize regions based on configuration
        self._initialize_regions()
    
    def _initialize_regions(self):
        """Initialize temporal regions based on configuration."""
        # For now, create a simple grid - will be overridden by specific implementations
        pass
    
    def should_sample(self, data_hash: str = None) -> bool:
        """Determine if a record should be sampled."""
        if len(self.samples) >= self.config.max_sample_size:
            # Reservoir sampling: replace random existing sample
            return random.random() < self.config.sampling_rate
        
        # Simple probability-based sampling
        return random.random() < self.config.sampling_rate
    
    def add_sample(self, vt: float, tt: float, data: Dict[str, Any]):
        """Add a sample to the statistics collector."""
        if not self.should_sample():
            return
        
        # Calculate temporal weight
        weight = self.config.get_temporal_weight(tt)
        
        # Reservoir sampling if at capacity
        if len(self.samples) >= self.config.max_sample_size:
            replace_idx = random.randint(0, len(self.samples) - 1)
            self.samples[replace_idx] = (vt, tt, data)
        else:
            self.samples.append((vt, tt, data))
        
        # Update region statistics
        for region in self.regions:
            if region.contains_point(vt, tt):
                region.update_with_sample(vt, tt, data, weight)
        
        # Update global statistics
        for attr, value in data.items():
            if attr != 'payload':
                if attr not in self.global_attribute_stats:
                    self.global_attribute_stats[attr] = AttributeStats()
                self.global_attribute_stats[attr].update_with_value(value, weight)
        
        self.total_records_processed += 1
    
    async def add_samples(self, rectangles):
        """Add multiple samples from rectangles."""
        for rect in rectangles:
            if self.should_sample():
                # Convert datetime to timestamp if needed
                vt_timestamp = rect.vt_from.timestamp() if hasattr(rect.vt_from, 'timestamp') else float(rect.vt_from)
                tt_timestamp = rect.tt_from.timestamp() if hasattr(rect.tt_from, 'timestamp') else float(rect.tt_from)
                self.add_sample(vt_timestamp, tt_timestamp, rect.data)
    
    def get_attribute_selectivity(self, attribute: str, value: Any, vt: float = None, tt: float = None) -> float:
        """Get estimated selectivity for an attribute value in a specific temporal region."""
        # Find the most relevant region
        if vt is not None and tt is not None:
            for region in self.regions:
                if region.contains_point(vt, tt) and attribute in region.attribute_stats:
                    return region.attribute_stats[attribute].estimate_selectivity_for_value(value)
        
        # Fall back to global statistics
        if attribute in self.global_attribute_stats:
            return self.global_attribute_stats[attribute].estimate_selectivity_for_value(value)
        
        # Default assumption: moderately selective
        return 0.5
    
    def get_optimal_predicate_order(self, predicates: List[Tuple[str, Any]], vt: float = None, tt: float = None) -> List[Tuple[str, Any]]:
        """Order predicates by estimated selectivity (most selective first)."""
        if not predicates:
            return predicates
        
        # Calculate selectivity for each predicate
        predicate_selectivity = []
        for attr, value in predicates:
            selectivity = self.get_attribute_selectivity(attr, value, vt, tt)
            predicate_selectivity.append((selectivity, attr, value))
        
        # Sort by selectivity (highest first) and return reordered predicates
        predicate_selectivity.sort(reverse=True)
        return [(attr, value) for _, attr, value in predicate_selectivity]
    
    def update_statistics(self, force: bool = False):
        """Update statistics if needed."""
        data_change_ratio = 0.0  # Simplified for now
        
        if force or self.config.should_update_statistics(self.last_statistics_update, data_change_ratio):
            self._rebuild_statistics()
            self.last_statistics_update = time.time()
    
    def _rebuild_statistics(self):
        """Rebuild statistics from current samples."""
        # Clear existing statistics
        for region in self.regions:
            region.attribute_stats.clear()
            region.sample_count = 0
        
        self.global_attribute_stats.clear()
        
        # Rebuild from samples
        for vt, tt, data in self.samples:
            weight = self.config.get_temporal_weight(tt)
            
            # Update regions
            for region in self.regions:
                if region.contains_point(vt, tt):
                    region.update_with_sample(vt, tt, data, weight)
            
            # Update global stats
            for attr, value in data.items():
                if attr != 'payload':
                    if attr not in self.global_attribute_stats:
                        self.global_attribute_stats[attr] = AttributeStats()
                    self.global_attribute_stats[attr].update_with_value(value, weight)
    
    def get_statistics_summary(self) -> Dict[str, Any]:
        """Get a summary of current statistics."""
        return {
            'total_samples': len(self.samples),
            'total_records_processed': self.total_records_processed,
            'sampling_rate_actual': len(self.samples) / max(1, self.total_records_processed),
            'regions_count': len(self.regions),
            'last_update': self.last_statistics_update,
            'global_attributes': list(self.global_attribute_stats.keys()),
            'region_sample_distribution': [region.sample_count for region in self.regions]
        }


class EnhancedGridBasedStatisticsCollector(BitemporalStatisticsCollector):
    """Enhanced grid-based statistics collector with improved functionality."""
    
    def __init__(self, config: OptimizationConfig):
        super().__init__(config)
        
        # Grid-specific attributes
        self.vt_grid_size = None
        self.tt_grid_size = None
        self.grid_initialized = False
        self.grid_regions: Dict[Tuple[int, int], TemporalRegion] = {}
        
        print(f"EnhancedGridBasedStatisticsCollector: Using {config.grid_vt_cells}x{config.grid_tt_cells} grid")
    
    def _initialize_grid(self, vt_min: float, vt_max: float, tt_min: float, tt_max: float):
        """Initialize the fixed temporal grid."""
        if self.grid_initialized:
            return
        
        self.vt_grid_size = (vt_max - vt_min) / self.config.grid_vt_cells
        self.tt_grid_size = (tt_max - tt_min) / self.config.grid_tt_cells
        self.vt_min = vt_min
        self.tt_min = tt_min
        
        # Create grid regions
        for i in range(self.config.grid_vt_cells):
            for j in range(self.config.grid_tt_cells):
                vt_start = vt_min + i * self.vt_grid_size
                vt_end = vt_start + self.vt_grid_size
                tt_start = tt_min + j * self.tt_grid_size
                tt_end = tt_start + self.tt_grid_size
                
                region = TemporalRegion(
                    vt_min=vt_start,
                    vt_max=vt_end,
                    tt_min=tt_start,
                    tt_max=tt_end
                )
                self.grid_regions[(i, j)] = region
        
        self.grid_initialized = True
        print(f"Grid initialized: VT range [{vt_min:.2f}, {vt_max:.2f}], TT range [{tt_min:.2f}, {tt_max:.2f}]")
    
    def _get_grid_cell(self, vt: float, tt: float) -> Optional[Tuple[int, int]]:
        """Get grid cell coordinates for given temporal point."""
        if not self.grid_initialized:
            return None
        
        vt_cell = int((vt - self.vt_min) / self.vt_grid_size)
        tt_cell = int((tt - self.tt_min) / self.tt_grid_size)
        
        # Clamp to grid bounds
        vt_cell = max(0, min(vt_cell, self.config.grid_vt_cells - 1))
        tt_cell = max(0, min(tt_cell, self.config.grid_tt_cells - 1))
        
        return (vt_cell, tt_cell)
    
    def add_sample(self, vt: float, tt: float, data: Dict[str, Any]):
        """Add sample to grid-based statistics."""
        # Initialize grid if this is the first sample
        if not self.grid_initialized:
            # Use initial bounds, will expand as needed
            self._initialize_grid(vt - 1000, vt + 1000, tt - 1000, tt + 1000)
        
        # Call parent method for base functionality
        super().add_sample(vt, tt, data)
        
        # Get grid cell and update grid-specific statistics
        grid_cell = self._get_grid_cell(vt, tt)
        if grid_cell and grid_cell in self.grid_regions:
            region = self.grid_regions[grid_cell]
            region.last_updated = time.time()
    
    def get_region_id(self, vt: float, tt: float) -> Optional[str]:
        """Get region identifier for given temporal point."""
        grid_cell = self._get_grid_cell(vt, tt)
        if grid_cell:
            return f"grid_{grid_cell[0]}_{grid_cell[1]}"
        return None


class EnhancedAdaptiveStatisticsCollector(BitemporalStatisticsCollector):
    """Enhanced adaptive statistics collector with improved region management."""
    
    def __init__(self, config: OptimizationConfig):
        super().__init__(config)
        
        # Adaptive regions
        self.adaptive_regions: Dict[str, TemporalRegion] = {}
        self.region_counter = 0
        
        # Query pattern tracking for adaptation
        self.query_hotspots = []
        self.adaptation_threshold = getattr(config, 'min_region_size', 10)
        
        print(f"EnhancedAdaptiveStatisticsCollector: Max {getattr(config, 'max_regions', 100)} adaptive regions")
    
    def _create_adaptive_region(self, vt: float, tt: float, initial_size: float = 100.0) -> str:
        """Create a new adaptive region around the given point."""
        max_regions = getattr(self.config, 'max_regions', 100)
        if len(self.adaptive_regions) >= max_regions:
            # Remove least recently used region
            lru_region_id = min(self.adaptive_regions.keys(), 
                              key=lambda rid: self.adaptive_regions[rid].last_updated)
            del self.adaptive_regions[lru_region_id]
        
        region_id = f"adaptive_{self.region_counter}"
        self.region_counter += 1
        
        region = TemporalRegion(
            vt_min=vt - initial_size/2,
            vt_max=vt + initial_size/2,
            tt_min=tt - initial_size/2,
            tt_max=tt + initial_size/2
        )
        
        self.adaptive_regions[region_id] = region
        return region_id
    
    def _find_containing_region(self, vt: float, tt: float) -> Optional[str]:
        """Find adaptive region containing the given point."""
        for region_id, region in self.adaptive_regions.items():
            if region.contains_point(vt, tt):
                return region_id
        return None
    
    def _adapt_regions(self):
        """Adapt regions based on query patterns and data distribution."""
        if len(self.query_hotspots) < 10:
            return False
        
        # Analyze recent query patterns
        recent_queries = self.query_hotspots[-50:]  # Last 50 queries
        
        # Find clusters of queries
        clusters = self._find_query_clusters(recent_queries)
        
        # Create or adjust regions based on clusters
        adapted = False
        for cluster_center, cluster_size in clusters:
            vt_center, tt_center = cluster_center
            
            # Check if we need a new region for this cluster
            existing_region = self._find_containing_region(vt_center, tt_center)
            
            if not existing_region and cluster_size >= self.adaptation_threshold:
                # Create new region for this hotspot
                self._create_adaptive_region(vt_center, tt_center, cluster_size * 2)
                adapted = True
        
        return adapted
    
    def _find_query_clusters(self, queries: List[Tuple[float, float]]) -> List[Tuple[Tuple[float, float], float]]:
        """Simple clustering to find query hotspots."""
        if not queries:
            return []
        
        # Simple distance-based clustering
        clusters = []
        processed = set()
        
        for i, (vt1, tt1) in enumerate(queries):
            if i in processed:
                continue
            
            cluster_points = [(vt1, tt1)]
            processed.add(i)
            
            # Find nearby points
            for j, (vt2, tt2) in enumerate(queries):
                if j in processed:
                    continue
                
                distance = ((vt1 - vt2) ** 2 + (tt1 - tt2) ** 2) ** 0.5
                if distance < 50.0:  # Threshold for clustering
                    cluster_points.append((vt2, tt2))
                    processed.add(j)
            
            if len(cluster_points) >= 3:  # Minimum cluster size
                # Calculate cluster center and size
                vt_center = sum(vt for vt, tt in cluster_points) / len(cluster_points)
                tt_center = sum(tt for vt, tt in cluster_points) / len(cluster_points)
                
                # Calculate cluster radius
                max_distance = max(((vt - vt_center) ** 2 + (tt - tt_center) ** 2) ** 0.5 
                                 for vt, tt in cluster_points)
                
                clusters.append(((vt_center, tt_center), max_distance))
        
        return clusters
    
    def add_sample(self, vt: float, tt: float, data: Dict[str, Any]):
        """Add sample to adaptive statistics."""
        # Track query location for adaptation
        self.query_hotspots.append((vt, tt))
        if len(self.query_hotspots) > 1000:  # Keep recent history
            self.query_hotspots = self.query_hotspots[-1000:]
        
        # Find or create containing region
        region_id = self._find_containing_region(vt, tt)
        if not region_id:
            region_id = self._create_adaptive_region(vt, tt)
        
        # Update region statistics
        if region_id in self.adaptive_regions:
            region = self.adaptive_regions[region_id]
            region.last_updated = time.time()
        
        # Call parent method for base functionality
        super().add_sample(vt, tt, data)
    
    def update_statistics(self, force: bool = False):
        """Update statistics and potentially adapt regions."""
        super().update_statistics(force)
        
        # Adapt regions based on patterns
        self._adapt_regions()
    
    def get_region_id(self, vt: float, tt: float) -> Optional[str]:
        """Get region identifier for given temporal point."""
        return self._find_containing_region(vt, tt)
    
    def estimate_query_selectivity(self, predicates: List[Tuple[str, Any]], vt: float, tt: float) -> float:
        """Estimate query selectivity based on adaptive region statistics."""
        region_id = self._find_containing_region(vt, tt)
        if not region_id or region_id not in self.adaptive_regions:
            return 0.5  # Default selectivity
        
        region = self.adaptive_regions[region_id]
        if region.sample_count == 0:
            return 0.5
        
        # Calculate selectivity based on attribute statistics
        total_selectivity = 1.0
        for attr, value in predicates:
            attr_selectivity = self.get_attribute_selectivity(attr, value, vt, tt)
            total_selectivity *= attr_selectivity
        
        return total_selectivity
    
    def get_index_recommendation(self, predicates: List[Tuple[str, Any]], vt: float, tt: float) -> Optional[str]:
        """Get index recommendation based on adaptive analysis."""
        if not predicates:
            return None
        
        # Find most selective attribute
        best_attr = None
        best_selectivity = 0.0
        
        for attr, value in predicates:
            selectivity = self.get_attribute_selectivity(attr, value, vt, tt)
            if selectivity > best_selectivity:
                best_selectivity = selectivity
                best_attr = attr
        
        selectivity_threshold = getattr(self.config, 'selectivity_threshold', 0.1)
        if best_attr and best_selectivity > selectivity_threshold:
            return f"idx_{best_attr}"
        
        return None
    
    def estimate_temporal_selectivity(self, vt_from: float, vt_to: float, tt_from: float, tt_to: float) -> float:
        """Estimate selectivity for temporal range queries."""
        # Convert datetime objects to timestamps if needed
        if hasattr(vt_from, 'timestamp'):
            vt_from = vt_from.timestamp()
        if hasattr(vt_to, 'timestamp'):
            vt_to = vt_to.timestamp()
        if hasattr(tt_from, 'timestamp'):
            tt_from = tt_from.timestamp()
        if hasattr(tt_to, 'timestamp'):
            tt_to = tt_to.timestamp()
            
        if not self.adaptive_regions:
            return 0.5  # Default moderate selectivity
        
        total_samples = sum(region.sample_count for region in self.adaptive_regions.values())
        if total_samples == 0:
            return 0.5
        
        # Count samples in the temporal range
        matching_samples = 0
        for region in self.adaptive_regions.values():
            # Check if region overlaps with query range
            if (region.vt_min <= vt_to and region.vt_max >= vt_from and
                region.tt_min <= tt_to and region.tt_max >= tt_from):
                # Estimate overlap ratio
                vt_overlap = min(vt_to, region.vt_max) - max(vt_from, region.vt_min)
                tt_overlap = min(tt_to, region.tt_max) - max(tt_from, region.tt_min)
                region_vt_span = region.vt_max - region.vt_min
                region_tt_span = region.tt_max - region.tt_min
                
                if region_vt_span > 0 and region_tt_span > 0:
                    overlap_ratio = (vt_overlap * tt_overlap) / (region_vt_span * region_tt_span)
                    matching_samples += region.sample_count * overlap_ratio
        
        return min(1.0, matching_samples / total_samples)
    
    def get_locality_score(self, vt_from: float, vt_to: float, tt_from: float, tt_to: float) -> float:
        """Calculate locality score for a given temporal range."""
        if not self.adaptive_regions:
            return 0.5  # Default moderate locality
        
        # Convert datetime objects to timestamps if needed
        if hasattr(vt_from, 'timestamp'):
            vt_from = vt_from.timestamp()
        if hasattr(vt_to, 'timestamp'):
            vt_to = vt_to.timestamp()
        if hasattr(tt_from, 'timestamp'):
            tt_from = tt_from.timestamp()
        if hasattr(tt_to, 'timestamp'):
            tt_to = tt_to.timestamp()
        
        # Calculate center point of the query range
        vt_center = (vt_from + vt_to) / 2
        tt_center = (tt_from + tt_to) / 2
        
        # Find regions that overlap with the query range
        overlapping_regions = []
        for region in self.adaptive_regions.values():
            if (region.vt_min <= vt_to and region.vt_max >= vt_from and
                region.tt_min <= tt_to and region.tt_max >= tt_from):
                overlapping_regions.append(region)
        
        if not overlapping_regions:
            return 0.1  # Low locality for ranges outside known regions
        
        # Calculate locality based on sample density in overlapping regions
        total_samples = sum(region.sample_count for region in self.adaptive_regions.values())
        if total_samples == 0:
            return 0.5
        
        overlapping_samples = sum(region.sample_count for region in overlapping_regions)
        region_density = overlapping_samples / total_samples
        
        # Normalize to 0-1 range, with higher density indicating better locality
        return min(1.0, region_density * len(self.adaptive_regions))

# Use the enhanced implementations above
GridBasedStatisticsCollector = EnhancedGridBasedStatisticsCollector
AdaptiveStatisticsCollector = EnhancedAdaptiveStatisticsCollector