import hashlib
import json
import uuid
import time
from typing import Dict, Any, List, Tuple, Optional
import duckdb
from .solutionD import SolutionD
from .bitemporal_space import Rectangle
from .optimization_config import OptimizationConfig, get_config
from .bitemporal_statistics import EnhancedAdaptiveStatisticsCollector as AdaptiveStatisticsCollector

class SolutionD_AdaptiveOptimized(SolutionD):
    """SolutionD with Phase 2 optimization: Adaptive temporal regions and advanced selectivity estimation."""
    
    def __init__(self, config: OptimizationConfig = None) -> None:
        super().__init__()
        self.name = "SolutionD_AdaptiveOptimized"
        
        # Load optimization configuration
        self.opt_config = config if config else get_config()
        
        # Validate configuration
        config_errors = self.opt_config.validate_config()
        if config_errors:
            raise ValueError(f"Invalid configuration: {', '.join(config_errors)}")
        
        # Initialize adaptive statistics collector
        self.stats_collector = AdaptiveStatisticsCollector(self.opt_config)
        
        # Performance tracking
        self.query_count = 0
        self.optimization_hits = 0
        self.total_optimization_time = 0.0
        self.region_adaptations = 0
        
        # Query pattern tracking for adaptive optimization
        self.query_patterns = []
        self.hot_regions = set()
        
        print(f"{self.name}: Initialized with adaptive optimization (sampling rate: {self.opt_config.sampling_rate:.1%})")
    
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Insert rectangles and update adaptive statistics."""
        # Call parent implementation
        await super().insert_rectangle_to_collections(rectangles, entity)
        
        # Update statistics with new data
        for rect in rectangles:
            # Extract temporal coordinates
            vt = rect.vt_from.timestamp() if hasattr(rect.vt_from, 'timestamp') else float(rect.vt_from)
            tt = rect.tt_from.timestamp() if hasattr(rect.tt_from, 'timestamp') else float(rect.tt_from)
            
            # Add sample to adaptive statistics collector
            self.stats_collector.add_sample(vt, tt, rect.data["payload"])
        
        # Update statistics and potentially adapt regions
        adaptation_occurred = self.stats_collector.update_statistics()
        if adaptation_occurred:
            self.region_adaptations += 1
    
    def _track_query_pattern(self, vt: float, tt: float, predicates: List[Tuple[str, Any]]) -> None:
        """Track query patterns for adaptive optimization."""
        pattern = {
            'vt': vt,
            'tt': tt,
            'predicates': predicates,
            'timestamp': time.time()
        }
        
        self.query_patterns.append(pattern)
        
        # Keep only recent patterns (last 1000 queries)
        if len(self.query_patterns) > 1000:
            self.query_patterns = self.query_patterns[-1000:]
        
        # Update hot regions based on query frequency
        self._update_hot_regions()
    
    def _update_hot_regions(self) -> None:
        """Update hot regions based on recent query patterns."""
        if len(self.query_patterns) < 10:
            return
        
        # Analyze recent query patterns to identify hot regions
        recent_patterns = self.query_patterns[-100:]  # Last 100 queries
        region_counts = {}
        
        for pattern in recent_patterns:
            region_id = self.stats_collector.get_region_id(pattern['vt'], pattern['tt'])
            if region_id:
                region_counts[region_id] = region_counts.get(region_id, 0) + 1
        
        # Identify hot regions (top 20% by query frequency)
        if region_counts:
            sorted_regions = sorted(region_counts.items(), key=lambda x: x[1], reverse=True)
            hot_threshold = max(1, len(sorted_regions) // 5)  # Top 20%
            self.hot_regions = {region_id for region_id, _ in sorted_regions[:hot_threshold]}
    
    def _optimize_query_predicates_adaptive(self, predicates: List[Tuple[str, Any]], vt: float, tt: float) -> Tuple[List[Tuple[str, Any]], Dict[str, Any]]:
        """Advanced predicate optimization with adaptive region awareness."""
        start_time = time.time()
        
        # Track this query pattern
        self._track_query_pattern(vt, tt, predicates)
        
        # Get region-specific optimization
        region_id = self.stats_collector.get_region_id(vt, tt)
        is_hot_region = region_id in self.hot_regions
        
        # Get optimal predicate order with region context
        optimized_predicates = self.stats_collector.get_optimal_predicate_order(predicates, vt, tt)
        
        # Get additional optimization hints
        optimization_hints = {
            'region_id': region_id,
            'is_hot_region': is_hot_region,
            'estimated_selectivity': self.stats_collector.estimate_query_selectivity(predicates, vt, tt),
            'recommended_index_hint': self.stats_collector.get_index_recommendation(predicates, vt, tt)
        }
        
        # Track optimization performance
        optimization_time = time.time() - start_time
        self.total_optimization_time += optimization_time
        
        # Check if optimization changed the order
        if optimized_predicates != predicates:
            self.optimization_hits += 1
        
        return optimized_predicates, optimization_hints
    
    def _build_adaptive_query(self, base_query: str, optimization_hints: Dict[str, Any]) -> str:
        """Build query with adaptive optimizations based on hints."""
        query = base_query
        
        # Add index hints for hot regions or highly selective queries
        if optimization_hints.get('is_hot_region') or optimization_hints.get('estimated_selectivity', 0) > self.opt_config.selectivity_threshold:
            index_hint = optimization_hints.get('recommended_index_hint')
            if index_hint:
                # Add DuckDB-specific optimization hints
                query = f"/*+ USE_INDEX({index_hint}) */ {query}"
        
        return query
    
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Adaptive optimized point query with region-aware optimization."""
        if not self.connection:
            await self.connect()
        
        self.query_count += 1
        
        # Track memory usage
        initial_memory = self._get_memory_usage()
        
        # Convert temporal coordinates for optimization
        vt_float = vt.timestamp() if hasattr(vt, 'timestamp') else float(vt)
        tt_float = tt.timestamp() if hasattr(tt, 'timestamp') else float(tt)
        
        # Adaptive predicate optimization
        predicates = [('name', name), ('age', age)]
        optimized_predicates, optimization_hints = self._optimize_query_predicates_adaptive(predicates, vt_float, tt_float)
        
        # Build base temporal conditions
        base_conditions = [
            "i_ts.entity = ?",
            "i_ts.tt_from <= ?",
            "i_ts.tt_to > ?",
            "i_ts.vt_from <= ?",
            "i_ts.vt_to > ?"
        ]
        
        # Build optimized WHERE clause
        where_conditions = base_conditions.copy()
        params = [entity, tt, tt, vt, vt]
        
        for attr, value in optimized_predicates:
            where_conditions.append(f"i_data.{attr} = ?")
            params.append(value)
        
        # Construct base query
        base_query = f"""
        WITH matched_indices AS (
            SELECT i_ts.vref
            FROM Index_ts i_ts
            JOIN Index_data i_data ON i_ts.vref = i_data.vref
            WHERE {' AND '.join(where_conditions)}
        )
        SELECT i_data.data
        FROM Index_data i_data
        JOIN matched_indices m ON i_data.vref = m.vref
        """
        
        # Apply adaptive optimizations
        optimized_query = self._build_adaptive_query(base_query, optimization_hints)
        
        # Execute query
        results = self.connection.execute(optimized_query, params).fetchall()
        
        # Log performance if enabled
        if self.opt_config.enable_performance_tracking and self.query_count % self.opt_config.performance_log_interval == 0:
            self._log_adaptive_performance_stats()
        
        # Log memory usage
        final_memory = self._get_memory_usage()
        memory_used = final_memory - initial_memory
        
        if self.opt_config.enable_performance_tracking:
            print(f"{self.name} Adaptive Query Memory Usage: {memory_used:.2f} MB")
            if optimization_hints.get('is_hot_region'):
                print(f"  Query executed in hot region: {optimization_hints['region_id']}")
        
        # Parse JSON data from results
        return [json.loads(row[0]) for row in results]
    
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Adaptive optimized range query with region-aware optimization."""
        if not self.connection:
            await self.connect()
        
        self.query_count += 1
        
        # Track memory usage
        initial_memory = self._get_memory_usage()
        
        # Convert temporal coordinates for optimization (use midpoint)
        vt_mid = ((vt_from.timestamp() if hasattr(vt_from, 'timestamp') else float(vt_from)) + 
                  (vt_to.timestamp() if hasattr(vt_to, 'timestamp') else float(vt_to))) / 2
        tt_mid = ((tt_from.timestamp() if hasattr(tt_from, 'timestamp') else float(tt_from)) + 
                  (tt_to.timestamp() if hasattr(tt_to, 'timestamp') else float(tt_to))) / 2
        
        # Adaptive predicate optimization
        predicates = [('name', name), ('age', age)]
        optimized_predicates, optimization_hints = self._optimize_query_predicates_adaptive(predicates, vt_mid, tt_mid)
        
        # Build base temporal conditions
        base_conditions = [
            "i_ts.entity = ?",
            "i_ts.tt_from < ?",
            "i_ts.tt_to > ?",
            "i_ts.vt_from < ?",
            "i_ts.vt_to > ?"
        ]
        
        # Build optimized WHERE clause
        where_conditions = base_conditions.copy()
        params = [entity, tt_to, tt_from, vt_to, vt_from]
        
        for attr, value in optimized_predicates:
            where_conditions.append(f"i_data.{attr} = ?")
            params.append(value)
        
        # Construct base query
        base_query = f"""
        WITH matched_indices AS (
            SELECT i_ts.vref
            FROM Index_ts i_ts
            JOIN Index_data i_data ON i_ts.vref = i_data.vref
            WHERE {' AND '.join(where_conditions)}
        )
        SELECT i_data.data
        FROM Index_data i_data
        JOIN matched_indices m ON i_data.vref = m.vref
        """
        
        # Apply adaptive optimizations
        optimized_query = self._build_adaptive_query(base_query, optimization_hints)
        
        # Execute query
        results = self.connection.execute(optimized_query, params).fetchall()
        
        # Log performance if enabled
        if self.opt_config.enable_performance_tracking and self.query_count % self.opt_config.performance_log_interval == 0:
            self._log_adaptive_performance_stats()
        
        # Log memory usage
        final_memory = self._get_memory_usage()
        memory_used = final_memory - initial_memory
        
        if self.opt_config.enable_performance_tracking:
            print(f"{self.name} Adaptive Range Query Memory Usage: {memory_used:.2f} MB")
            if optimization_hints.get('is_hot_region'):
                print(f"  Query executed in hot region: {optimization_hints['region_id']}")
        
        # Parse JSON data from results
        return [json.loads(row[0]) for row in results]
    
    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Adaptive optimized single attribute range query."""
        if not self.connection:
            await self.connect()
        
        self.query_count += 1
        
        # Track memory usage
        initial_memory = self._get_memory_usage()
        
        # Convert temporal coordinates for optimization (use midpoint)
        vt_mid = ((vt_from.timestamp() if hasattr(vt_from, 'timestamp') else float(vt_from)) + 
                  (vt_to.timestamp() if hasattr(vt_to, 'timestamp') else float(vt_to))) / 2
        tt_mid = ((tt_from.timestamp() if hasattr(tt_from, 'timestamp') else float(tt_from)) + 
                  (tt_to.timestamp() if hasattr(tt_to, 'timestamp') else float(tt_to))) / 2
        
        # Adaptive predicate optimization
        predicates = [(attribute_name, attribute_value)]
        optimized_predicates, optimization_hints = self._optimize_query_predicates_adaptive(predicates, vt_mid, tt_mid)
        
        # Build base temporal conditions
        base_conditions = [
            "i_ts.entity = ?",
            "i_ts.tt_from < ?",
            "i_ts.tt_to > ?",
            "i_ts.vt_from < ?",
            "i_ts.vt_to > ?"
        ]
        
        # Determine optimal condition order based on selectivity
        estimated_selectivity = optimization_hints.get('estimated_selectivity', 0)
        
        if estimated_selectivity > self.opt_config.selectivity_threshold:
            # High selectivity: put attribute condition first
            where_conditions = [f"i_data.{attribute_name} = ?"] + base_conditions
            params = [attribute_value, entity, tt_to, tt_from, vt_to, vt_from]
        else:
            # Low selectivity: put temporal conditions first
            where_conditions = base_conditions + [f"i_data.{attribute_name} = ?"]
            params = [entity, tt_to, tt_from, vt_to, vt_from, attribute_value]
        
        # Construct base query
        base_query = f"""
        WITH matched_indices AS (
            SELECT i_ts.vref
            FROM Index_ts i_ts
            JOIN Index_data i_data ON i_ts.vref = i_data.vref
            WHERE {' AND '.join(where_conditions)}
        )
        SELECT i_data.data
        FROM Index_data i_data
        JOIN matched_indices m ON i_data.vref = m.vref
        """
        
        # Apply adaptive optimizations
        optimized_query = self._build_adaptive_query(base_query, optimization_hints)
        
        # Execute query
        results = self.connection.execute(optimized_query, params).fetchall()
        
        # Log performance if enabled
        if self.opt_config.enable_performance_tracking and self.query_count % self.opt_config.performance_log_interval == 0:
            self._log_adaptive_performance_stats()
        
        # Log memory usage
        final_memory = self._get_memory_usage()
        memory_used = final_memory - initial_memory
        
        if self.opt_config.enable_performance_tracking:
            print(f"{self.name} Adaptive Attribute Query Memory Usage: {memory_used:.2f} MB")
            if optimization_hints.get('is_hot_region'):
                print(f"  Query executed in hot region: {optimization_hints['region_id']}")
        
        # Parse JSON data from results
        return [json.loads(row[0]) for row in results]
    
    async def _query_at_point(self, name, vt, tt, entity="Student"):
        """Adaptive optimized helper method for point queries."""
        if not self.connection:
            await self.connect()
        
        # Convert temporal coordinates for optimization
        vt_float = vt.timestamp() if hasattr(vt, 'timestamp') else float(vt)
        tt_float = tt.timestamp() if hasattr(tt, 'timestamp') else float(tt)
        
        # Adaptive predicate optimization
        predicates = [('name', name)]
        optimized_predicates, optimization_hints = self._optimize_query_predicates_adaptive(predicates, vt_float, tt_float)
        
        # Build base temporal conditions
        base_conditions = [
            "i_ts.entity = ?",
            "i_ts.tt_from <= ?",
            "i_ts.tt_to > ?",
            "i_ts.vt_from <= ?",
            "i_ts.vt_to > ?"
        ]
        
        # Build optimized WHERE clause
        where_conditions = base_conditions.copy()
        params = [entity, tt, tt, vt, vt]
        
        for attr, value in optimized_predicates:
            where_conditions.append(f"i_data.{attr} = ?")
            params.append(value)
        
        try:
            # Construct base query
            base_query = f"""
            WITH matched_indices AS (
                SELECT i_ts.vref
                FROM Index_ts i_ts
                JOIN Index_data i_data ON i_ts.vref = i_data.vref
                WHERE {' AND '.join(where_conditions)}
                LIMIT 1
            )
            SELECT i_data.data
            FROM Index_data i_data
            JOIN matched_indices m ON i_data.vref = m.vref
            """
            
            # Apply adaptive optimizations
            optimized_query = self._build_adaptive_query(base_query, optimization_hints)
            
            results = self.connection.execute(optimized_query, params).fetchall()
            
            if results:
                return json.loads(results[0][0])
            return None
            
        except Exception:
            return None
    
    def _log_adaptive_performance_stats(self):
        """Log adaptive optimization performance statistics."""
        if self.query_count == 0:
            return
        
        optimization_rate = (self.optimization_hits / self.query_count) * 100
        avg_optimization_time = (self.total_optimization_time / self.query_count) * 1000  # ms
        
        stats_summary = self.stats_collector.get_statistics_summary()
        
        print(f"{self.name} Adaptive Performance Stats:")
        print(f"  Queries processed: {self.query_count}")
        print(f"  Optimization hit rate: {optimization_rate:.1f}%")
        print(f"  Avg optimization time: {avg_optimization_time:.2f}ms")
        print(f"  Region adaptations: {self.region_adaptations}")
        print(f"  Hot regions: {len(self.hot_regions)}")
        print(f"  Total samples: {stats_summary['total_samples']}")
        print(f"  Actual sampling rate: {stats_summary['sampling_rate_actual']:.1%}")
        print(f"  Adaptive regions: {stats_summary['regions_count']}")
    
    def get_optimization_summary(self) -> Dict[str, Any]:
        """Get comprehensive adaptive optimization summary."""
        stats_summary = self.stats_collector.get_statistics_summary()
        
        return {
            'solution_name': self.name,
            'optimization_type': 'Adaptive (Phase 2)',
            'configuration': {
                'sampling_rate': self.opt_config.sampling_rate,
                'min_region_size': self.opt_config.min_region_size,
                'max_regions': self.opt_config.max_regions,
                'update_interval': self.opt_config.batch_update_interval_seconds
            },
            'performance': {
                'total_queries': self.query_count,
                'optimization_hits': self.optimization_hits,
                'optimization_rate': (self.optimization_hits / max(1, self.query_count)) * 100,
                'avg_optimization_time_ms': (self.total_optimization_time / max(1, self.query_count)) * 1000,
                'region_adaptations': self.region_adaptations,
                'hot_regions_count': len(self.hot_regions)
            },
            'statistics': stats_summary,
            'adaptive_features': {
                'query_patterns_tracked': len(self.query_patterns),
                'hot_regions': list(self.hot_regions),
                'recent_adaptations': self.region_adaptations
            }
        }