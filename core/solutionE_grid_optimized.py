from typing import Dict, Any, List
from collections import defaultdict
import duckdb
from .solutionE import SolutionE
from .bitemporal_statistics import GridBasedStatisticsCollector
from .optimization_config import OptimizationConfig, get_config
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class SolutionE_GridOptimized(SolutionE):
    """Grid-optimized version of SolutionE with DuckDB query optimization"""
    
    def __init__(self, config: OptimizationConfig = None) -> None:
        super().__init__()
        self.name = "SolutionE_GridOptimized"
        self.config = config if config else get_config()
        self.stats_collector = GridBasedStatisticsCollector(self.config)
        # Field-specific statistics for optimization
        self.field_stats = {
            "name": {
                "selectivity_estimates": [],
                "value_distribution": defaultdict(int),
                "query_count": 0
            },
            "age": {
                "selectivity_estimates": [],
                "value_distribution": defaultdict(int),
                "query_count": 0
            },
            "temporal": {
                "selectivity_estimates": [],
                "range_queries": 0,
                "point_queries": 0
            }
        }
        
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Enhanced insertion with statistics collection"""
        # Collect statistics for optimization
        await self.stats_collector.add_samples(rectangles)
        
        # Perform original insertion
        await super().insert_rectangle_to_collections(rectangles, entity)
        
        # Update field statistics
        await self._update_field_statistics(rectangles)
        
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Grid-optimized query with dynamic SQL optimization"""
        if not self.connection:
            await self.connect()
            
        # Get temporal selectivity estimate
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt, vt, tt, tt)
        
        # Update query statistics
        self.field_stats["name"]["query_count"] += 1
        self.field_stats["age"]["query_count"] += 1
        self.field_stats["temporal"]["point_queries"] += 1
        
        # Build optimized SQL query
        sql_query = self._build_optimized_point_query_sql(
            name, age, tt, vt, entity, temporal_selectivity
        )
        
        with Timer(f"{self.name}_point_query") as timer:
            results = self.connection.execute(sql_query).fetchall()
            
        print(f"{self.name}: Grid-optimized point query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Grid-optimized range query with enhanced SQL optimization"""
        if not self.connection:
            await self.connect()
            
        # Get temporal selectivity estimate
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        
        # Update query statistics
        self.field_stats["name"]["query_count"] += 1
        self.field_stats["age"]["query_count"] += 1
        self.field_stats["temporal"]["range_queries"] += 1
        
        # Build optimized SQL query
        sql_query = self._build_optimized_range_query_sql(
            name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity
        )
        
        with Timer(f"{self.name}_range_query") as timer:
            results = self.connection.execute(sql_query).fetchall()
            
        print(f"{self.name}: Grid-optimized range query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Grid-optimized attribute range query with field-specific optimization"""
        if not self.connection:
            await self.connect()
            
        # Get temporal selectivity estimate
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        
        # Update query statistics
        field_key = attribute_name.lower()
        if field_key in self.field_stats:
            self.field_stats[field_key]["query_count"] += 1
        self.field_stats["temporal"]["range_queries"] += 1
        
        # Build optimized single-attribute SQL query
        sql_query = self._build_optimized_attribute_query_sql(
            attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity
        )
        
        with Timer(f"{self.name}_attribute_query") as timer:
            results = self.connection.execute(sql_query).fetchall()
            
        print(f"{self.name}: Grid-optimized attribute query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def _update_field_statistics(self, rectangles: List[Rectangle]):
        """Update field statistics for optimization"""
        for rect in rectangles:
            payload = rect.data.get("payload", {})
            
            # Update name field statistics
            if "name" in payload:
                name_value = str(payload["name"])
                self.field_stats["name"]["value_distribution"][name_value] += 1
                
            # Update age field statistics
            if "age" in payload:
                age_value = str(payload["age"])
                self.field_stats["age"]["value_distribution"][age_value] += 1
                
    def _estimate_field_selectivity(self, field_name: str, field_value: str) -> float:
        """Estimate selectivity for a specific field value"""
        if field_name not in self.field_stats:
            return 0.5  # Default selectivity
            
        stats = self.field_stats[field_name]
        value_count = stats["value_distribution"].get(field_value, 0)
        total_count = sum(stats["value_distribution"].values())
        
        if total_count == 0:
            return 0.5
            
        # Calculate selectivity (lower value count = higher selectivity)
        selectivity = 1.0 - (value_count / total_count)
        return max(0.01, min(0.99, selectivity))
        
    def _build_optimized_point_query_sql(self, name, age, tt, vt, entity, temporal_selectivity):
        """Build optimized SQL query for point queries"""
        # Calculate field selectivities
        name_selectivity = self._estimate_field_selectivity("name", str(name))
        age_selectivity = self._estimate_field_selectivity("age", str(age))
        
        # Determine optimal predicate order (most selective first)
        predicates = [
            ("name", name_selectivity, f"name = '{name}'"),
            ("age", age_selectivity, f"age = '{age}'"),
            ("temporal", temporal_selectivity, f"vt_from <= {vt} AND vt_to > {vt} AND tt_from <= {tt} AND tt_to > {tt}")
        ]
        predicates.sort(key=lambda x: x[1], reverse=True)
        
        # Build WHERE clause with optimal predicate order
        where_conditions = [pred[2] for pred in predicates]
        where_conditions.append(f"entity = '{entity}'")
        
        # Add index hints based on most selective field
        most_selective_field = predicates[0][0]
        if most_selective_field == "name":
            index_hint = "/*+ USE_INDEX(name_entity_idx) */"
        elif most_selective_field == "age":
            index_hint = "/*+ USE_INDEX(age_temporal_idx) */"
        elif most_selective_field == "temporal":
            index_hint = "/*+ USE_INDEX(temporal_idx) */"
        else:
            index_hint = ""
            
        sql_query = f"""
        SELECT {index_hint} *
        FROM {entity}
        WHERE {' AND '.join(where_conditions)}
        """
        
        return sql_query
        
    def _build_optimized_range_query_sql(self, name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity):
        """Build optimized SQL query for range queries"""
        # Calculate field selectivities
        name_selectivity = self._estimate_field_selectivity("name", str(name))
        age_selectivity = self._estimate_field_selectivity("age", str(age))
        
        # Determine optimal predicate order
        predicates = [
            ("name", name_selectivity, f"name = '{name}'"),
            ("age", age_selectivity, f"age = '{age}'"),
            ("temporal", temporal_selectivity, f"vt_from < {vt_to} AND vt_to > {vt_from} AND tt_from < {tt_to} AND tt_to > {tt_from}")
        ]
        predicates.sort(key=lambda x: x[1], reverse=True)
        
        # Build WHERE clause with optimal predicate order
        where_conditions = [pred[2] for pred in predicates]
        where_conditions.append(f"entity = '{entity}'")
        
        # Add appropriate index hints
        most_selective_field = predicates[0][0]
        if most_selective_field == "temporal" and temporal_selectivity > 0.3:
            index_hint = "/*+ USE_INDEX(temporal_range_idx) */"
        elif most_selective_field == "name":
            index_hint = "/*+ USE_INDEX(name_entity_idx) */"
        elif most_selective_field == "age":
            index_hint = "/*+ USE_INDEX(age_temporal_idx) */"
        else:
            index_hint = ""
            
        sql_query = f"""
        SELECT {index_hint} *
        FROM {entity}
        WHERE {' AND '.join(where_conditions)}
        """
        
        return sql_query
        
    def _build_optimized_attribute_query_sql(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity):
        """Build optimized SQL query for single attribute queries"""
        # Calculate attribute selectivity
        field_key = attribute_name.lower()
        attribute_selectivity = self._estimate_field_selectivity(field_key, str(attribute_value))
        
        # Determine predicate order
        temporal_condition = f"vt_from < {vt_to} AND vt_to > {vt_from} AND tt_from < {tt_to} AND tt_to > {tt_from}"
        attribute_condition = f"{attribute_name.lower()} = '{attribute_value}'"
        entity_condition = f"entity = '{entity}'"
        
        if attribute_selectivity > temporal_selectivity:
            # Attribute is more selective - filter by attribute first
            where_conditions = [attribute_condition, entity_condition, temporal_condition]
            index_hint = f"/*+ USE_INDEX({attribute_name.lower()}_entity_idx) */"
        else:
            # Temporal is more selective - filter by temporal first
            where_conditions = [temporal_condition, attribute_condition, entity_condition]
            index_hint = "/*+ USE_INDEX(temporal_range_idx) */"
            
        sql_query = f"""
        SELECT {index_hint} *
        FROM {entity}
        WHERE {' AND '.join(where_conditions)}
        """
        
        return sql_query