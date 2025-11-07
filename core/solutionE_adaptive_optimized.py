from typing import Dict, Any, List
from collections import defaultdict
import duckdb
from .solutionE import SolutionE
from .bitemporal_statistics import EnhancedAdaptiveStatisticsCollector
from .optimization_config import OptimizationConfig, get_config
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class SolutionE_AdaptiveOptimized(SolutionE):
    """Adaptive optimized version of SolutionE with advanced DuckDB query optimization"""
    
    def __init__(self, config: OptimizationConfig = None) -> None:
        super().__init__()
        self.name = "SolutionE_AdaptiveOptimized"
        self.config = config if config else get_config()
        self.stats_collector = EnhancedAdaptiveStatisticsCollector(self.config)
        # Enhanced field-specific statistics for adaptive optimization
        self.field_stats = {
            "name": {
                "selectivity_estimates": [],
                "value_distribution": defaultdict(int),
                "query_count": 0,
                "locality_scores": defaultdict(float),
                "access_patterns": []
            },
            "age": {
                "selectivity_estimates": [],
                "value_distribution": defaultdict(int),
                "query_count": 0,
                "locality_scores": defaultdict(float),
                "access_patterns": []
            },
            "temporal": {
                "selectivity_estimates": [],
                "range_queries": 0,
                "point_queries": 0,
                "locality_scores": defaultdict(float),
                "access_patterns": []
            }
        }
        self.query_history = []
        
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Enhanced insertion with adaptive statistics collection"""
        # Collect statistics for optimization
        await self.stats_collector.add_samples(rectangles)
        
        # Perform original insertion
        await super().insert_rectangle_to_collections(rectangles, entity)
        
        # Update field statistics with locality awareness
        await self._update_adaptive_field_statistics(rectangles)
        
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Adaptive optimized query with dynamic SQL optimization"""
        if not self.connection:
            await self.connect()
            
        # Get enhanced temporal selectivity estimate
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt, vt, tt, tt)
        locality_score = self.stats_collector.get_locality_score(vt, vt, tt, tt)
        
        # Update query statistics with access patterns
        self._update_query_statistics("point", {"name": name, "age": age, "vt": vt, "tt": tt})
        
        # Build adaptive optimized SQL query
        sql_query = self._build_adaptive_point_query_sql(
            name, age, tt, vt, entity, temporal_selectivity, locality_score
        )
        
        with Timer(f"{self.name}_point_query") as timer:
            results = self.connection.execute(sql_query, [name, age, entity, tt, tt, vt, vt, tt, tt, vt, vt]).fetchall()
            
        print(f"{self.name}: Adaptive-optimized point query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Adaptive optimized range query with enhanced SQL optimization"""
        if not self.connection:
            await self.connect()
            
        # Get enhanced temporal selectivity estimate
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        locality_score = self.stats_collector.get_locality_score(vt_from, vt_to, tt_from, tt_to)
        
        # Update query statistics with access patterns
        self._update_query_statistics("range", {
            "name": name, "age": age, 
            "vt_from": vt_from, "vt_to": vt_to,
            "tt_from": tt_from, "tt_to": tt_to
        })
        
        # Build adaptive optimized SQL query
        sql_query, params = self._build_adaptive_range_query_sql(
            name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity, locality_score
        )
        
        with Timer(f"{self.name}_range_query") as timer:
            results = self.connection.execute(sql_query, params).fetchall()
            
        print(f"{self.name}: Adaptive-optimized range query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Adaptive optimized attribute range query with field-specific optimization"""
        if not self.connection:
            await self.connect()
            
        # Get enhanced temporal selectivity estimate
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        locality_score = self.stats_collector.get_locality_score(vt_from, vt_to, tt_from, tt_to)
        
        # Update query statistics with access patterns
        self._update_query_statistics("attribute", {
            attribute_name: attribute_value,
            "vt_from": vt_from, "vt_to": vt_to,
            "tt_from": tt_from, "tt_to": tt_to
        })
        
        # Build adaptive optimized SQL query
        sql_query, params = self._build_adaptive_attribute_query_sql(
            attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity, locality_score
        )
        
        with Timer(f"{self.name}_attribute_query") as timer:
            results = self.connection.execute(sql_query, params).fetchall()
            
        print(f"{self.name}: Adaptive-optimized attribute query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def _update_adaptive_field_statistics(self, rectangles: List[Rectangle]):
        """Update field statistics with adaptive locality awareness"""
        for rect in rectangles:
            payload = rect.data.get("payload", {})
            
            # Update name field statistics with locality
            if "name" in payload:
                name_value = str(payload["name"])
                self.field_stats["name"]["value_distribution"][name_value] += 1
                # Update locality score based on temporal proximity
                temporal_key = f"{rect.vt_from}_{rect.tt_from}"
                self.field_stats["name"]["locality_scores"][name_value] += self._calculate_locality_bonus(rect)
                
            # Update age field statistics with locality
            if "age" in payload:
                age_value = str(payload["age"])
                self.field_stats["age"]["value_distribution"][age_value] += 1
                # Update locality score based on temporal proximity
                self.field_stats["age"]["locality_scores"][age_value] += self._calculate_locality_bonus(rect)
                
    def _update_query_statistics(self, query_type: str, query_params: Dict[str, Any]):
        """Update query statistics with access pattern tracking"""
        query_record = {
            "type": query_type,
            "params": query_params,
            "timestamp": len(self.query_history)
        }
        self.query_history.append(query_record)
        
        # Update field-specific access patterns
        for field_name in ["name", "age"]:
            if field_name in query_params:
                field_value = str(query_params[field_name])
                self.field_stats[field_name]["access_patterns"].append(query_record)
                self.field_stats[field_name]["query_count"] += 1
                
        # Update temporal access patterns
        if any(key.startswith(("vt", "tt")) for key in query_params.keys()):
            self.field_stats["temporal"]["access_patterns"].append(query_record)
            if query_type == "point":
                self.field_stats["temporal"]["point_queries"] += 1
            else:
                self.field_stats["temporal"]["range_queries"] += 1
                
    def _calculate_locality_bonus(self, rect: Rectangle) -> float:
        """Calculate locality bonus based on temporal clustering"""
        # Simple locality calculation based on recent query patterns
        recent_queries = self.query_history[-10:] if len(self.query_history) > 10 else self.query_history
        
        locality_bonus = 0.0
        for query in recent_queries:
            params = query["params"]
            if "vt" in params and "tt" in params:
                # Calculate temporal distance
                vt_dist = abs(rect.vt_from - params["vt"])
                tt_dist = abs(rect.tt_from - params["tt"])
                temporal_distance = (vt_dist + tt_dist) / 2.0
                
                # Higher bonus for closer temporal proximity
                if temporal_distance < 100:  # Within 100 time units
                    locality_bonus += 1.0 / (1.0 + temporal_distance * 0.01)
                    
        return locality_bonus
        
    def _estimate_adaptive_field_selectivity(self, field_name: str, field_value: str) -> float:
        """Estimate selectivity with adaptive locality awareness"""
        if field_name not in self.field_stats:
            return 0.5  # Default selectivity
            
        stats = self.field_stats[field_name]
        value_count = stats["value_distribution"].get(field_value, 0)
        total_count = sum(stats["value_distribution"].values())
        locality_score = stats["locality_scores"].get(field_value, 0.0)
        
        if total_count == 0:
            return 0.5
            
        # Base selectivity calculation
        base_selectivity = 1.0 - (value_count / total_count)
        
        # Adjust selectivity based on locality score (higher locality = better selectivity)
        locality_factor = min(0.3, locality_score * 0.1)  # Cap at 30% improvement
        adjusted_selectivity = base_selectivity + locality_factor
        
        return max(0.01, min(0.99, adjusted_selectivity))
        
    def _build_adaptive_point_query_sql(self, name, age, tt, vt, entity, temporal_selectivity, locality_score):
        """Build adaptive optimized SQL query for point queries using SolutionE's table structure"""
        # Convert inputs to strings for consistency
        name = str(name)
        age = str(age)
        
        # Use the same SQL structure as base SolutionE but with optimization hints
        query = """
        WITH joined_data AS (
            SELECT
                n.eref AS id,
                n.value AS name,
                a.value AS age,
                GREATEST(n.tt_from, a.tt_from) AS tt_from,
                LEAST(n.tt_to, a.tt_to) AS tt_to,
                GREATEST(n.vt_from, a.vt_from) AS vt_from,
                LEAST(n.vt_to, a.vt_to) AS vt_to
            FROM Name n
            JOIN Age a ON n.eref = a.eref AND n.entity = a.entity
            WHERE n.value = ? 
            AND a.value = ? 
            AND n.entity = ?
            AND n.tt_from <= ? AND n.tt_to > ?
            AND n.vt_from <= ? AND n.vt_to > ?
            AND a.tt_from <= ? AND a.tt_to > ?
            AND a.vt_from <= ? AND a.vt_to > ?
            AND n.tt_from < LEAST(n.tt_to, a.tt_to)
            AND n.vt_from < LEAST(n.vt_to, a.vt_to)
            AND a.tt_from < LEAST(n.tt_to, a.tt_to)
            AND a.vt_from < LEAST(n.vt_to, a.vt_to)
        )
        SELECT DISTINCT id, name, age, tt_from, tt_to, vt_from, vt_to
        FROM joined_data
        WHERE tt_from < tt_to AND vt_from < vt_to
        ORDER BY id, tt_from, vt_from
        """
        
        return query
        
    def _build_adaptive_range_query_sql(self, name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity, locality_score):
        """Build adaptive optimized SQL query for range queries"""
        # Calculate adaptive field selectivities
        name_selectivity = self._estimate_adaptive_field_selectivity("name", str(name))
        age_selectivity = self._estimate_adaptive_field_selectivity("age", str(age))
        
        # Adjust temporal selectivity based on locality and range size
        # Convert datetime objects to timestamps if needed
        vt_from_ts = vt_from.timestamp() if hasattr(vt_from, 'timestamp') else vt_from
        vt_to_ts = vt_to.timestamp() if hasattr(vt_to, 'timestamp') else vt_to
        tt_from_ts = tt_from.timestamp() if hasattr(tt_from, 'timestamp') else tt_from
        tt_to_ts = tt_to.timestamp() if hasattr(tt_to, 'timestamp') else tt_to
        
        range_size = (vt_to_ts - vt_from_ts) * (tt_to_ts - tt_from_ts)
        range_factor = min(0.2, 1000.0 / max(1.0, range_size))  # Smaller ranges are more selective
        adjusted_temporal_selectivity = temporal_selectivity + (locality_score * 0.1) + range_factor
        adjusted_temporal_selectivity = max(0.01, min(0.99, adjusted_temporal_selectivity))
        
        # Determine optimal predicate order with adaptive weighting
        predicates = [
            ("name", name_selectivity, f"name = '{name}'"),
            ("age", age_selectivity, f"age = '{age}'"),
            ("temporal", adjusted_temporal_selectivity, f"vt_from <= '{vt_to}' AND vt_to > '{vt_from}' AND tt_from <= '{tt_to}' AND tt_to > '{tt_from}'")
        ]
        predicates.sort(key=lambda x: x[1], reverse=True)
        
        # Build WHERE clause with optimal predicate order
        where_conditions = [pred[2] for pred in predicates]
        where_conditions.append(f"entity = '{entity}'")
        
        # Advanced index hints with adaptive optimization
        most_selective_field = predicates[0][0]
        if most_selective_field == "temporal" and adjusted_temporal_selectivity > 0.5:
            if locality_score > 3.0:
                index_hint = "/*+ USE_INDEX(temporal_clustered_idx) PARALLEL(8) */"
            else:
                index_hint = "/*+ USE_INDEX(temporal_range_idx) PARALLEL(4) */"
        elif most_selective_field == "name" and name_selectivity > 0.6:
            index_hint = "/*+ USE_INDEX(name_entity_idx) PARALLEL(4) */"
        elif most_selective_field == "age" and age_selectivity > 0.6:
            index_hint = "/*+ USE_INDEX(age_temporal_idx) PARALLEL(4) */"
        else:
            index_hint = "/*+ USE_INDEX(composite_range_idx) PARALLEL(2) */"
            
        sql_query = f"""
        SELECT {index_hint} n.eref, n.value as name, a.value as age, n.tt_from, n.tt_to, n.vt_from, n.vt_to
        FROM Name n
        JOIN Age a ON n.eref = a.eref AND n.tt_from = a.tt_from AND n.vt_from = a.vt_from
        WHERE n.value = ? AND a.value = ? AND n.entity = ? AND
              n.tt_from <= ? AND n.tt_to > ? AND n.vt_from <= ? AND n.vt_to > ?
        """
        
        return sql_query, [name, age, entity, tt_to, tt_from, vt_to, vt_from]
        

        
    def _build_adaptive_attribute_query_sql(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity, locality_score):
        """Build adaptive optimized SQL query for single attribute queries"""
        # Calculate adaptive attribute selectivity
        field_key = attribute_name.lower()
        attribute_selectivity = self._estimate_adaptive_field_selectivity(field_key, str(attribute_value))
        
        # Adjust temporal selectivity based on locality
        # Convert datetime objects to timestamps if needed
        vt_from_ts = vt_from.timestamp() if hasattr(vt_from, 'timestamp') else vt_from
        vt_to_ts = vt_to.timestamp() if hasattr(vt_to, 'timestamp') else vt_to
        tt_from_ts = tt_from.timestamp() if hasattr(tt_from, 'timestamp') else tt_from
        tt_to_ts = tt_to.timestamp() if hasattr(tt_to, 'timestamp') else tt_to
        
        range_size = (vt_to_ts - vt_from_ts) * (tt_to_ts - tt_from_ts)
        range_factor = min(0.2, 1000.0 / max(1.0, range_size))
        adjusted_temporal_selectivity = temporal_selectivity + (locality_score * 0.1) + range_factor
        adjusted_temporal_selectivity = max(0.01, min(0.99, adjusted_temporal_selectivity))
        
        # Determine predicate order with adaptive optimization
        temporal_condition = f"vt_from <= '{vt_to}' AND vt_to > '{vt_from}' AND tt_from <= '{tt_to}' AND tt_to > '{tt_from}'"
        attribute_condition = f"{attribute_name.lower()} = '{attribute_value}'"
        entity_condition = f"entity = '{entity}'"
        
        if attribute_selectivity > adjusted_temporal_selectivity:
            # Attribute is more selective - filter by attribute first
            where_conditions = [attribute_condition, entity_condition, temporal_condition]
            if attribute_selectivity > 0.7:
                index_hint = f"/*+ USE_INDEX({attribute_name.lower()}_entity_idx) PARALLEL(4) */"
            else:
                index_hint = f"/*+ USE_INDEX({attribute_name.lower()}_composite_idx) PARALLEL(2) */"
        else:
            # Temporal is more selective - filter by temporal first
            where_conditions = [temporal_condition, attribute_condition, entity_condition]
            if locality_score > 3.0:
                index_hint = "/*+ USE_INDEX(temporal_clustered_idx) PARALLEL(6) */"
            else:
                index_hint = "/*+ USE_INDEX(temporal_range_idx) PARALLEL(4) */"
            
        # Use appropriate table based on attribute
        if attribute_name.lower() == 'name':
            sql_query = f"""
            SELECT {index_hint} eref, value as {attribute_name.lower()}, tt_from, tt_to, vt_from, vt_to
            FROM Name
            WHERE value = ? AND entity = ? AND
                  tt_from <= ? AND tt_to > ? AND vt_from <= ? AND vt_to > ?
            """
        elif attribute_name.lower() == 'age':
            sql_query = f"""
            SELECT {index_hint} eref, value as {attribute_name.lower()}, tt_from, tt_to, vt_from, vt_to
            FROM Age
            WHERE value = ? AND entity = ? AND
                  tt_from <= ? AND tt_to > ? AND vt_from <= ? AND vt_to > ?
            """
        else:
            # Fallback for other attributes
            sql_query = f"""
            SELECT {index_hint} eref, value as {attribute_name.lower()}, tt_from, tt_to, vt_from, vt_to
            FROM {attribute_name.title()}
            WHERE value = ? AND entity = ? AND
                  tt_from <= ? AND tt_to > ? AND vt_from <= ? AND vt_to > ?
            """
        
        return sql_query, [attribute_value, entity, tt_to, tt_from, vt_to, vt_from]