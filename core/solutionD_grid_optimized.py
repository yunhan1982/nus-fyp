import hashlib
import json
import uuid
import time
from typing import Dict, Any, List, Tuple, Optional
import duckdb
from .solutionD import SolutionD
from .bitemporal_space import Rectangle
from .optimization_config import OptimizationConfig, get_config
from .bitemporal_statistics import EnhancedGridBasedStatisticsCollector as GridBasedStatisticsCollector

class SolutionD_GridOptimized(SolutionD):
    """SolutionD with Phase 1 optimization: Fixed temporal grid sampling and basic selectivity estimation."""
    
    def __init__(self, config: OptimizationConfig = None) -> None:
        super().__init__()
        self.name = "SolutionD_GridOptimized"
        
        # Load optimization configuration
        self.opt_config = config if config else get_config()
        
        # Validate configuration
        config_errors = self.opt_config.validate_config()
        if config_errors:
            raise ValueError(f"Invalid configuration: {', '.join(config_errors)}")
        
        # Initialize statistics collector
        self.stats_collector = GridBasedStatisticsCollector(self.opt_config)
        
        # Performance tracking
        self.query_count = 0
        self.optimization_hits = 0
        self.total_optimization_time = 0.0
        
        print(f"{self.name}: Initialized with grid-based optimization (sampling rate: {self.opt_config.sampling_rate:.1%})")
    
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Insert rectangles and update statistics."""
        # Call parent implementation
        await super().insert_rectangle_to_collections(rectangles, entity)
        
        # Update statistics with new data
        for rect in rectangles:
            # Extract temporal coordinates
            vt = rect.vt_from.timestamp() if hasattr(rect.vt_from, 'timestamp') else float(rect.vt_from)
            tt = rect.tt_from.timestamp() if hasattr(rect.tt_from, 'timestamp') else float(rect.tt_from)
            
            # Add sample to statistics collector
            self.stats_collector.add_sample(vt, tt, rect.data["payload"])
        
        # Update statistics if needed
        self.stats_collector.update_statistics()
    
    def _optimize_query_predicates(self, predicates: List[Tuple[str, Any]], vt: float = None, tt: float = None) -> List[Tuple[str, Any]]:
        """Optimize predicate order based on selectivity statistics."""
        start_time = time.time()
        
        # Get optimal predicate order from statistics
        optimized_predicates = self.stats_collector.get_optimal_predicate_order(predicates, vt, tt)
        
        # Track optimization performance
        optimization_time = time.time() - start_time
        self.total_optimization_time += optimization_time
        
        # Check if optimization changed the order
        if optimized_predicates != predicates:
            self.optimization_hits += 1
        
        return optimized_predicates
    
    def _build_optimized_where_clause(self, predicates: List[Tuple[str, Any]], base_conditions: List[str]) -> Tuple[str, List[Any]]:
        """Build WHERE clause with optimized predicate order."""
        if not predicates:
            return " AND ".join(base_conditions), []
        
        # Build optimized WHERE conditions
        where_conditions = base_conditions.copy()
        params = []
        
        for attr, value in predicates:
            where_conditions.append(f"i_data.{attr} = ?")
            params.append(value)
        
        return " AND ".join(where_conditions), params
    
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Optimized point query with predicate reordering."""
        if not self.connection:
            await self.connect()
        
        self.query_count += 1
        
        # Track memory usage
        initial_memory = self._get_memory_usage()
        
        # Convert temporal coordinates for optimization
        vt_float = vt.timestamp() if hasattr(vt, 'timestamp') else float(vt)
        tt_float = tt.timestamp() if hasattr(tt, 'timestamp') else float(tt)
        
        # Optimize predicate order
        predicates = [('name', name), ('age', age)]
        optimized_predicates = self._optimize_query_predicates(predicates, vt_float, tt_float)
        
        # Build base temporal conditions
        base_conditions = [
            "i_ts.entity = ?",
            "i_ts.tt_from <= ?",
            "i_ts.tt_to > ?",
            "i_ts.vt_from <= ?",
            "i_ts.vt_to > ?"
        ]
        
        # Build optimized WHERE clause
        where_clause, predicate_params = self._build_optimized_where_clause(optimized_predicates, base_conditions)
        
        # Construct optimized query
        query = f"""
        WITH matched_indices AS (
            SELECT i_ts.vref
            FROM Index_ts i_ts
            JOIN Index_data i_data ON i_ts.vref = i_data.vref
            WHERE {where_clause}
        )
        SELECT i_data.data
        FROM Index_data i_data
        JOIN matched_indices m ON i_data.vref = m.vref
        """
        
        # Combine all parameters in optimized order
        all_params = [entity, tt, tt, vt, vt] + predicate_params
        
        # Execute query
        results = self.connection.execute(query, all_params).fetchall()
        
        # Log performance if enabled
        if self.opt_config.enable_performance_tracking and self.query_count % self.opt_config.performance_log_interval == 0:
            self._log_performance_stats()
        
        # Log memory usage
        final_memory = self._get_memory_usage()
        memory_used = final_memory - initial_memory
        
        if self.opt_config.enable_performance_tracking:
            print(f"{self.name} Optimized Query Memory Usage: {memory_used:.2f} MB")
        
        # Parse JSON data from results
        return [json.loads(row[0]) for row in results]
    
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Optimized range query with predicate reordering."""
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
        
        # Optimize predicate order
        predicates = [('name', name), ('age', age)]
        optimized_predicates = self._optimize_query_predicates(predicates, vt_mid, tt_mid)
        
        # Build base temporal conditions
        base_conditions = [
            "i_ts.entity = ?",
            "i_ts.tt_from < ?",
            "i_ts.tt_to > ?",
            "i_ts.vt_from < ?",
            "i_ts.vt_to > ?"
        ]
        
        # Build optimized WHERE clause
        where_clause, predicate_params = self._build_optimized_where_clause(optimized_predicates, base_conditions)
        
        # Construct optimized query
        query = f"""
        WITH matched_indices AS (
            SELECT i_ts.vref
            FROM Index_ts i_ts
            JOIN Index_data i_data ON i_ts.vref = i_data.vref
            WHERE {where_clause}
        )
        SELECT i_data.data
        FROM Index_data i_data
        JOIN matched_indices m ON i_data.vref = m.vref
        """
        
        # Combine all parameters in optimized order
        all_params = [entity, tt_to, tt_from, vt_to, vt_from] + predicate_params
        
        # Execute query
        results = self.connection.execute(query, all_params).fetchall()
        
        # Log performance if enabled
        if self.opt_config.enable_performance_tracking and self.query_count % self.opt_config.performance_log_interval == 0:
            self._log_performance_stats()
        
        # Log memory usage
        final_memory = self._get_memory_usage()
        memory_used = final_memory - initial_memory
        
        if self.opt_config.enable_performance_tracking:
            print(f"{self.name} Optimized Range Query Memory Usage: {memory_used:.2f} MB")
        
        # Parse JSON data from results
        return [json.loads(row[0]) for row in results]
    
    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Optimized single attribute range query."""
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
        
        # For single attribute queries, check if we should add temporal constraints first
        attr_selectivity = self.stats_collector.get_attribute_selectivity(attribute_name, attribute_value, vt_mid, tt_mid)
        
        # Build base temporal conditions
        base_conditions = [
            "i_ts.entity = ?",
            "i_ts.tt_from < ?",
            "i_ts.tt_to > ?",
            "i_ts.vt_from < ?",
            "i_ts.vt_to > ?"
        ]
        
        # Add attribute condition - place it first if highly selective
        if attr_selectivity > self.opt_config.selectivity_threshold:
            where_conditions = [f"i_data.{attribute_name} = ?"] + base_conditions
            all_params = [attribute_value, entity, tt_to, tt_from, vt_to, vt_from]
        else:
            where_conditions = base_conditions + [f"i_data.{attribute_name} = ?"]
            all_params = [entity, tt_to, tt_from, vt_to, vt_from, attribute_value]
        
        # Construct optimized query
        query = f"""
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
        
        # Execute query
        results = self.connection.execute(query, all_params).fetchall()
        
        # Log performance if enabled
        if self.opt_config.enable_performance_tracking and self.query_count % self.opt_config.performance_log_interval == 0:
            self._log_performance_stats()
        
        # Log memory usage
        final_memory = self._get_memory_usage()
        memory_used = final_memory - initial_memory
        
        if self.opt_config.enable_performance_tracking:
            print(f"{self.name} Optimized Attribute Query Memory Usage: {memory_used:.2f} MB")
        
        # Parse JSON data from results
        return [json.loads(row[0]) for row in results]
    
    async def _query_at_point(self, name, vt, tt, entity="Student"):
        """Optimized helper method for point queries."""
        if not self.connection:
            await self.connect()
        
        # Convert temporal coordinates for optimization
        vt_float = vt.timestamp() if hasattr(vt, 'timestamp') else float(vt)
        tt_float = tt.timestamp() if hasattr(tt, 'timestamp') else float(tt)
        
        # Optimize predicate order (only name in this case)
        predicates = [('name', name)]
        optimized_predicates = self._optimize_query_predicates(predicates, vt_float, tt_float)
        
        # Build base temporal conditions
        base_conditions = [
            "i_ts.entity = ?",
            "i_ts.tt_from <= ?",
            "i_ts.tt_to > ?",
            "i_ts.vt_from <= ?",
            "i_ts.vt_to > ?"
        ]
        
        # Build optimized WHERE clause
        where_clause, predicate_params = self._build_optimized_where_clause(optimized_predicates, base_conditions)
        
        try:
            query = f"""
            WITH matched_indices AS (
                SELECT i_ts.vref
                FROM Index_ts i_ts
                JOIN Index_data i_data ON i_ts.vref = i_data.vref
                WHERE {where_clause}
                LIMIT 1
            )
            SELECT i_data.data
            FROM Index_data i_data
            JOIN matched_indices m ON i_data.vref = m.vref
            """
            
            # Combine all parameters in optimized order
            all_params = [entity, tt, tt, vt, vt] + predicate_params
            
            results = self.connection.execute(query, all_params).fetchall()
            
            if results:
                return json.loads(results[0][0])
            return None
            
        except Exception:
            return None
    
    def _log_performance_stats(self):
        """Log optimization performance statistics."""
        if self.query_count == 0:
            return
        
        optimization_rate = (self.optimization_hits / self.query_count) * 100
        avg_optimization_time = (self.total_optimization_time / self.query_count) * 1000  # ms
        
        stats_summary = self.stats_collector.get_statistics_summary()
        
        print(f"{self.name} Performance Stats:")
        print(f"  Queries processed: {self.query_count}")
        print(f"  Optimization hit rate: {optimization_rate:.1f}%")
        print(f"  Avg optimization time: {avg_optimization_time:.2f}ms")
        print(f"  Total samples: {stats_summary['total_samples']}")
        print(f"  Actual sampling rate: {stats_summary['sampling_rate_actual']:.1%}")
        print(f"  Grid regions: {stats_summary['regions_count']}")
    
    def get_optimization_summary(self) -> Dict[str, Any]:
        """Get comprehensive optimization summary."""
        stats_summary = self.stats_collector.get_statistics_summary()
        
        return {
            'solution_name': self.name,
            'optimization_type': 'Grid-based (Phase 1)',
            'configuration': {
                'sampling_rate': self.opt_config.sampling_rate,
                'grid_cells': f"{self.opt_config.grid_vt_cells}x{self.opt_config.grid_tt_cells}",
                'update_interval': self.opt_config.batch_update_interval_seconds
            },
            'performance': {
                'total_queries': self.query_count,
                'optimization_hits': self.optimization_hits,
                'optimization_rate': (self.optimization_hits / max(1, self.query_count)) * 100,
                'avg_optimization_time_ms': (self.total_optimization_time / max(1, self.query_count)) * 1000
            },
            'statistics': stats_summary
        }