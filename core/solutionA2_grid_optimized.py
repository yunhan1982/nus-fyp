from typing import Dict, Any, List
from collections import defaultdict
import motor.motor_asyncio
from .solutionA2 import SolutionA2
from .bitemporal_statistics import GridBasedStatisticsCollector
from .optimization_config import OptimizationConfig, get_config
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class SolutionA2_GridOptimized(SolutionA2):
    """Grid-optimized version of SolutionA2 with aggregation pipeline optimization"""
    
    def __init__(self, config: OptimizationConfig = None) -> None:
        super().__init__()
        self.name = "SolutionA2_GridOptimized"
        self.config = config if config else get_config()
        self.stats_collector = GridBasedStatisticsCollector(self.config)
        self.db = self.client[self.name]
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
        """Grid-optimized query with dynamic pipeline optimization"""
        if not self.client:
            await self.connect()
            
        # Get temporal selectivity estimate
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt, vt, tt, tt)
        
        # Update query statistics
        self.field_stats["name"]["query_count"] += 1
        self.field_stats["age"]["query_count"] += 1
        self.field_stats["temporal"]["point_queries"] += 1
        
        # Build optimized aggregation pipeline
        pipeline = self._build_optimized_point_query_pipeline(
            name, age, tt, vt, entity, temporal_selectivity
        )
        
        with Timer() as timer:
            cursor = self.db[entity].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Grid-optimized point query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Grid-optimized range query with enhanced pipeline optimization"""
        if not self.client:
            await self.connect()
            
        # Get temporal selectivity estimate
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        
        # Update query statistics
        self.field_stats["name"]["query_count"] += 1
        self.field_stats["age"]["query_count"] += 1
        self.field_stats["temporal"]["range_queries"] += 1
        
        # Build optimized pipeline
        pipeline = self._build_optimized_range_query_pipeline(
            name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity
        )
        
        with Timer() as timer:
            cursor = self.db[entity].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Grid-optimized range query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Grid-optimized attribute range query with field-specific optimization"""
        if not self.client:
            await self.connect()
            
        # Get temporal selectivity estimate
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        
        # Update query statistics
        field_key = attribute_name.lower()
        if field_key in self.field_stats:
            self.field_stats[field_key]["query_count"] += 1
        self.field_stats["temporal"]["range_queries"] += 1
        
        # Build optimized single-attribute pipeline
        pipeline = self._build_optimized_attribute_pipeline(
            attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity
        )
        
        with Timer() as timer:
            cursor = self.db[entity].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
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
        
    def _build_optimized_point_query_pipeline(self, name, age, tt, vt, entity, temporal_selectivity):
        """Build optimized aggregation pipeline for point queries"""
        # Calculate field selectivities
        name_selectivity = self._estimate_field_selectivity("name", str(name))
        age_selectivity = self._estimate_field_selectivity("age", str(age))
        
        # Determine optimal field order (most selective first)
        field_selectivities = [
            ("name", name_selectivity),
            ("age", age_selectivity),
            ("temporal", temporal_selectivity)
        ]
        field_selectivities.sort(key=lambda x: x[1], reverse=True)
        
        stages = []
        
        # Add index hint based on most selective field
        most_selective_field = field_selectivities[0][0]
        if most_selective_field == "name":
            stages.append({
                "$hint": {
                    "name": 1,
                    "entity": 1,
                    "vt_from": 1,
                    "vt_to": 1
                }
            })
        elif most_selective_field == "age":
            stages.append({
                "$hint": {
                    "age": 1,
                    "vt_from": 1,
                    "vt_to": 1
                }
            })
        elif most_selective_field == "temporal":
            stages.append({
                "$hint": {
                    "vt_from": 1,
                    "vt_to": 1,
                    "tt_from": 1,
                    "tt_to": 1
                }
            })
            
        # Build match stages in selectivity order
        for field_name, _ in field_selectivities:
            if field_name == "name":
                stages.append({
                    "$match": {
                        "name": str(name),
                        "entity": entity
                    }
                })
            elif field_name == "age":
                stages.append({
                    "$match": {
                        "age": str(age)
                    }
                })
            elif field_name == "temporal":
                stages.append({
                    "$match": {
                        "vt_from": {"$lte": vt},
                        "vt_to": {"$gt": vt},
                        "tt_from": {"$lte": tt},
                        "tt_to": {"$gt": tt}
                    }
                })
                
        return stages
        
    def _build_optimized_range_query_pipeline(self, name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity):
        """Build optimized aggregation pipeline for range queries"""
        # Calculate field selectivities
        name_selectivity = self._estimate_field_selectivity("name", str(name))
        age_selectivity = self._estimate_field_selectivity("age", str(age))
        
        # Determine optimal field order
        field_selectivities = [
            ("name", name_selectivity),
            ("age", age_selectivity),
            ("temporal", temporal_selectivity)
        ]
        field_selectivities.sort(key=lambda x: x[1], reverse=True)
        
        stages = []
        
        # Add appropriate index hint
        most_selective_field = field_selectivities[0][0]
        if most_selective_field == "temporal" and temporal_selectivity > 0.3:
            stages.append({
                "$hint": {
                    "vt_from": 1,
                    "vt_to": 1,
                    "tt_from": 1,
                    "tt_to": 1
                }
            })
        elif most_selective_field == "name":
            stages.append({
                "$hint": {
                    "name": 1,
                    "entity": 1,
                    "vt_from": 1
                }
            })
        elif most_selective_field == "age":
            stages.append({
                "$hint": {
                    "age": 1,
                    "vt_from": 1,
                    "vt_to": 1
                }
            })
            
        # Build match stages in optimal order
        for field_name, _ in field_selectivities:
            if field_name == "name":
                stages.append({
                    "$match": {
                        "name": str(name),
                        "entity": entity
                    }
                })
            elif field_name == "age":
                stages.append({
                    "$match": {
                        "age": str(age)
                    }
                })
            elif field_name == "temporal":
                stages.append({
                    "$match": {
                        "$and": [
                            {"vt_from": {"$lt": vt_to}},
                            {"vt_to": {"$gt": vt_from}},
                            {"tt_from": {"$lt": tt_to}},
                            {"tt_to": {"$gt": tt_from}}
                        ]
                    }
                })
                
        return stages
        
    def _build_optimized_attribute_pipeline(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity):
        """Build optimized pipeline for single attribute queries"""
        # Calculate attribute selectivity
        field_key = attribute_name.lower()
        attribute_selectivity = self._estimate_field_selectivity(field_key, str(attribute_value))
        
        stages = []
        
        # Determine which field to filter first
        if attribute_selectivity > temporal_selectivity:
            # Attribute is more selective - filter by attribute first
            stages.extend([
                {
                    "$hint": {
                        attribute_name.lower(): 1,
                        "entity": 1,
                        "vt_from": 1
                    }
                },
                {
                    "$match": {
                        attribute_name.lower(): str(attribute_value),
                        "entity": entity
                    }
                },
                {
                    "$match": {
                        "$and": [
                            {"vt_from": {"$lt": vt_to}},
                            {"vt_to": {"$gt": vt_from}},
                            {"tt_from": {"$lt": tt_to}},
                            {"tt_to": {"$gt": tt_from}}
                        ]
                    }
                }
            ])
        else:
            # Temporal is more selective - filter by temporal first
            stages.extend([
                {
                    "$hint": {
                        "vt_from": 1,
                        "vt_to": 1,
                        "tt_from": 1,
                        "tt_to": 1
                    }
                },
                {
                    "$match": {
                        "$and": [
                            {"vt_from": {"$lt": vt_to}},
                            {"vt_to": {"$gt": vt_from}},
                            {"tt_from": {"$lt": tt_to}},
                            {"tt_to": {"$gt": tt_from}}
                        ]
                    }
                },
                {
                    "$match": {
                        attribute_name.lower(): str(attribute_value),
                        "entity": entity
                    }
                }
            ])
            
        return stages