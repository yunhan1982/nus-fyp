from typing import Dict, Any, List
from collections import defaultdict
import motor.motor_asyncio
from .solutionB1 import SolutionB1
from .bitemporal_statistics import EnhancedGridBasedStatisticsCollector as GridBasedStatisticsCollector
from .optimization_config import OptimizationConfig, get_config
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class SolutionB1_GridOptimized(SolutionB1):
    """Grid-based optimized version of SolutionB1 with aggregation pipeline optimization"""
    
    def __init__(self, config: OptimizationConfig = None) -> None:
        super().__init__()
        self.name = "SolutionB1_GridOptimized"
        self.config = config if config else get_config()
        self.stats_collector = GridBasedStatisticsCollector(self.config)
        self.db = self.client[self.name]
        # Track field-specific statistics for optimization
        self.field_stats = {
            "name": {"selectivity": 0.5, "query_frequency": 0},
            "age": {"selectivity": 0.3, "query_frequency": 0},
            "temporal": {"selectivity": 0.2, "query_frequency": 0}
        }
        
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Enhanced insertion with statistics collection"""
        # Collect statistics from sample
        await self.stats_collector.add_samples(rectangles)
        
        # Perform original insertion
        await super().insert_rectangle_to_collections(rectangles, entity)
        
        # Update field-specific statistics
        await self._update_field_statistics(rectangles)
        
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Optimized query with dynamic aggregation pipeline optimization"""
        if not self.client:
            await self.connect()
            
        # Get selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt, vt, tt, tt)
        
        # Update query frequency for adaptive learning
        self.field_stats["name"]["query_frequency"] += 1
        self.field_stats["age"]["query_frequency"] += 1
        self.field_stats["temporal"]["query_frequency"] += 1
        
        # Build optimized aggregation pipeline
        pipeline = self._build_optimized_point_query_pipeline(
            name, age, tt, vt, entity, temporal_selectivity
        )
        
        with Timer() as timer:
            cursor = self.db[entity].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Point query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Optimized range query with enhanced pipeline optimization"""
        if not self.client:
            await self.connect()
            
        # Get selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        
        # Update query frequency
        self.field_stats["name"]["query_frequency"] += 1
        self.field_stats["age"]["query_frequency"] += 1
        self.field_stats["temporal"]["query_frequency"] += 1
        
        # Build optimized pipeline
        pipeline = self._build_optimized_range_query_pipeline(
            name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity
        )
        
        with Timer() as timer:
            cursor = self.db[entity].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Range query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Optimized attribute range query with field-specific optimization"""
        if not self.client:
            await self.connect()
            
        # Get selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        
        # Update query frequency for the specific attribute
        if attribute_name.lower() in self.field_stats:
            self.field_stats[attribute_name.lower()]["query_frequency"] += 1
        self.field_stats["temporal"]["query_frequency"] += 1
        
        # Build optimized single-attribute pipeline
        pipeline = self._build_optimized_attribute_pipeline(
            attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity
        )
        
        with Timer() as timer:
            cursor = self.db[entity].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Attribute query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def _update_field_statistics(self, rectangles: List[Rectangle]):
        """Update field-specific statistics for optimization"""
        name_values = set()
        age_values = set()
        
        for rect in rectangles:
            payload = rect.data.get("payload", {})
            if "name" in payload:
                name_values.add(str(payload["name"]))
            if "age" in payload:
                age_values.add(str(payload["age"]))
                
        # Update selectivity estimates based on unique value ratios
        total_rects = len(rectangles)
        if total_rects > 0:
            self.field_stats["name"]["selectivity"] = len(name_values) / total_rects
            self.field_stats["age"]["selectivity"] = len(age_values) / total_rects
            
    def _get_optimal_field_order(self, fields: List[str], temporal_selectivity: float) -> List[str]:
        """Determine optimal field ordering based on selectivity and query frequency"""
        field_scores = {}
        
        for field in fields:
            if field.lower() in self.field_stats:
                stats = self.field_stats[field.lower()]
                # Combine selectivity and query frequency for scoring
                selectivity_score = stats["selectivity"]
                frequency_score = min(stats["query_frequency"] * 0.01, 0.5)  # Cap frequency bonus
                field_scores[field] = selectivity_score + frequency_score
            else:
                field_scores[field] = 0.5  # Default score
                
        # Add temporal considerations
        if temporal_selectivity < 0.2:  # High temporal selectivity
            field_scores["temporal"] = 0.9  # Prioritize temporal filtering
        else:
            field_scores["temporal"] = temporal_selectivity
            
        # Sort fields by score (higher is better for filtering)
        sorted_fields = sorted(fields + ["temporal"], 
                             key=lambda x: field_scores.get(x.lower(), field_scores.get(x, 0.5)), 
                             reverse=True)
        
        return sorted_fields
        
    def _build_optimized_point_query_pipeline(self, name, age, tt, vt, entity, temporal_selectivity):
        """Build optimized aggregation pipeline for point queries"""
        # Determine optimal field ordering
        field_order = self._get_optimal_field_order(["name", "age"], temporal_selectivity)
        
        stages = []
        
        # Build match stages in optimal order
        for field in field_order:
            if field == "name":
                stages.append({
                    "$match": {
                        "name": str(name),
                        "entity": entity
                    }
                })
            elif field == "age":
                stages.append({
                    "$match": {
                        "age": str(age)
                    }
                })
            elif field == "temporal":
                stages.append({
                    "$match": {
                        "vt_from": {"$lte": vt},
                        "vt_to": {"$gt": vt},
                        "tt_from": {"$lte": tt},
                        "tt_to": {"$gt": tt}
                    }
                })
                
        # Add index hint for optimal performance
        if temporal_selectivity < 0.1:  # High temporal selectivity
            stages.insert(0, {
                "$hint": {
                    "vt_from": 1,
                    "vt_to": 1,
                    "tt_from": 1,
                    "tt_to": 1
                }
            })
        else:
            # Use compound index hint based on field order
            primary_field = field_order[0] if field_order[0] != "temporal" else field_order[1]
            if primary_field == "name":
                stages.insert(0, {
                    "$hint": {
                        "name": 1,
                        "entity": 1
                    }
                })
            elif primary_field == "age":
                stages.insert(0, {
                    "$hint": {
                        "age": 1
                    }
                })
                
        return stages
        
    def _build_optimized_range_query_pipeline(self, name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity):
        """Build optimized aggregation pipeline for range queries"""
        # Determine optimal field ordering
        field_order = self._get_optimal_field_order(["name", "age"], temporal_selectivity)
        
        stages = []
        
        # Calculate temporal range size for optimization decisions
        temporal_range_size = (vt_to - vt_from) * (tt_to - tt_from)
        
        # Build match stages in optimal order
        for field in field_order:
            if field == "name":
                stages.append({
                    "$match": {
                        "name": str(name),
                        "entity": entity
                    }
                })
            elif field == "age":
                stages.append({
                    "$match": {
                        "age": str(age)
                    }
                })
            elif field == "temporal":
                # Use optimized temporal range matching
                if temporal_range_size < 100:  # Small range - use precise matching
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
                else:  # Large range - use broader matching with post-filtering
                    stages.extend([
                        {
                            "$match": {
                                "vt_from": {"$lte": vt_to},
                                "tt_from": {"$lte": tt_to}
                            }
                        },
                        {
                            "$match": {
                                "vt_to": {"$gte": vt_from},
                                "tt_to": {"$gte": tt_from}
                            }
                        }
                    ])
                    
        # Add index optimization hints
        if temporal_selectivity < 0.2 and temporal_range_size < 100:
            stages.insert(0, {
                "$hint": {
                    "vt_from": 1,
                    "vt_to": 1,
                    "tt_from": 1,
                    "tt_to": 1
                }
            })
        else:
            # Use field-based index hint
            primary_field = field_order[0] if field_order[0] != "temporal" else field_order[1]
            if primary_field == "name":
                stages.insert(0, {
                    "$hint": {
                        "name": 1,
                        "entity": 1,
                        "vt_from": 1
                    }
                })
                
        return stages
        
    def _build_optimized_attribute_pipeline(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity):
        """Build optimized pipeline for single attribute queries"""
        # Determine optimal ordering for attribute and temporal filters
        attribute_selectivity = self.field_stats.get(attribute_name.lower(), {}).get("selectivity", 0.5)
        
        stages = []
        
        # Decide filter order based on selectivity comparison
        if temporal_selectivity < attribute_selectivity:
            # Temporal filter is more selective
            stages.extend([
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
        else:
            # Attribute filter is more selective
            stages.extend([
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
            
        # Add appropriate index hint
        if temporal_selectivity < 0.1:
            stages.insert(0, {
                "$hint": {
                    "vt_from": 1,
                    "vt_to": 1,
                    "tt_from": 1,
                    "tt_to": 1
                }
            })
        else:
            stages.insert(0, {
                "$hint": {
                    attribute_name.lower(): 1,
                    "entity": 1
                }
            })
            
        # Add result limiting for large result sets
        if temporal_selectivity > 0.5 and attribute_selectivity > 0.5:
            stages.append({
                "$limit": 10000  # Reasonable limit for performance
            })
            
        return stages