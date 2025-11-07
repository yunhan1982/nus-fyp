from typing import Dict, Any, List
import motor.motor_asyncio
from .solutionA1 import SolutionA1
from .bitemporal_statistics import EnhancedGridBasedStatisticsCollector as GridBasedStatisticsCollector
from .optimization_config import OptimizationConfig, get_config
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class SolutionA1_GridOptimized(SolutionA1):
    """Grid-based optimized version of SolutionA1 with statistics-driven query optimization"""
    
    def __init__(self, config: OptimizationConfig = None) -> None:
        super().__init__()
        self.name = "SolutionA1_GridOptimized"
        self.config = config if config else get_config()
        self.stats_collector = GridBasedStatisticsCollector(self.config)
        self.db = self.client[self.name]
        
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Enhanced insertion with statistics collection"""
        # Collect statistics from sample
        await self.stats_collector.add_samples(rectangles)
        
        # Perform original insertion
        await super().insert_rectangle_to_collections(rectangles, entity)
        
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Optimized query with statistics-based index hints"""
        if not self.client:
            await self.connect()
            
        # Get selectivity estimates for optimization
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt, vt, tt, tt)
        
        # Build optimized aggregation pipeline based on selectivity
        pipeline = self._build_optimized_pipeline_point_query(
            name, age, tt, vt, entity, temporal_selectivity
        )
        
        with Timer() as timer:
            cursor = self.db.Index.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Point query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Optimized range query with statistics-based optimization"""
        if not self.client:
            await self.connect()
            
        # Get selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        
        # Build optimized pipeline
        pipeline = self._build_optimized_pipeline_range_query(
            name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity
        )
        
        with Timer() as timer:
            cursor = self.db.Index.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Range query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Optimized attribute range query"""
        if not self.client:
            await self.connect()
            
        # Get selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        
        # Build optimized pipeline for attribute query
        pipeline = self._build_optimized_attribute_pipeline(
            attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity
        )
        
        with Timer() as timer:
            cursor = self.db.Index.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Attribute range query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    def _build_optimized_pipeline_point_query(self, name, age, tt, vt, entity, temporal_selectivity):
        """Build optimized aggregation pipeline for point queries"""
        # Determine optimal stage ordering based on selectivity
        stages = []
        
        # Always start with most selective temporal filter if high selectivity
        if temporal_selectivity < 0.1:  # High selectivity
            stages.extend([
                {
                    "$match": {
                        "vt_from": {"$lte": vt},
                        "vt_to": {"$gt": vt},
                        "tt_from": {"$lte": tt},
                        "tt_to": {"$gt": tt}
                    }
                },
                {
                    "$match": {
                        "name": name,
                        "age": age,
                        "entity": entity
                    }
                }
            ])
        else:
            # Low temporal selectivity - filter by attributes first
            stages.extend([
                {
                    "$match": {
                        "name": name,
                        "age": age,
                        "entity": entity
                    }
                },
                {
                    "$match": {
                        "vt_from": {"$lte": vt},
                        "vt_to": {"$gt": vt},
                        "tt_from": {"$lte": tt},
                        "tt_to": {"$gt": tt}
                    }
                }
            ])
            
        # Add lookup stage
        stages.append({
            "$lookup": {
                "from": "Payloads",
                "localField": "vref",
                "foreignField": "vref",
                "as": "payload_data"
            }
        })
        
        stages.append({"$unwind": "$payload_data"})
        
        return stages
        
    def _build_optimized_pipeline_range_query(self, name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity):
        """Build optimized aggregation pipeline for range queries"""
        stages = []
        
        # Optimize stage ordering based on temporal selectivity
        if temporal_selectivity < 0.2:  # High temporal selectivity
            stages.extend([
                {
                    "$match": {
                        "$or": [
                            {
                                "$and": [
                                    {"vt_from": {"$lt": vt_to}},
                                    {"vt_to": {"$gt": vt_from}}
                                ]
                            }
                        ]
                    }
                },
                {
                    "$match": {
                        "$or": [
                            {
                                "$and": [
                                    {"tt_from": {"$lt": tt_to}},
                                    {"tt_to": {"$gt": tt_from}}
                                ]
                            }
                        ]
                    }
                },
                {
                    "$match": {
                        "name": name,
                        "age": age,
                        "entity": entity
                    }
                }
            ])
        else:
            # Low temporal selectivity - filter by attributes first
            stages.extend([
                {
                    "$match": {
                        "name": name,
                        "age": age,
                        "entity": entity
                    }
                },
                {
                    "$match": {
                        "$or": [
                            {
                                "$and": [
                                    {"vt_from": {"$lt": vt_to}},
                                    {"vt_to": {"$gt": vt_from}},
                                    {"tt_from": {"$lt": tt_to}},
                                    {"tt_to": {"$gt": tt_from}}
                                ]
                            }
                        ]
                    }
                }
            ])
            
        # Add lookup and projection stages
        stages.extend([
            {
                "$lookup": {
                    "from": "Payloads",
                    "localField": "vref",
                    "foreignField": "vref",
                    "as": "payload_data"
                }
            },
            {"$unwind": "$payload_data"}
        ])
        
        return stages
        
    def _build_optimized_attribute_pipeline(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity):
        """Build optimized pipeline for attribute queries"""
        stages = []
        
        # Create attribute match condition
        attr_match = {attribute_name: attribute_value, "entity": entity}
        
        if temporal_selectivity < 0.2:
            # High temporal selectivity - filter temporally first
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
                {"$match": attr_match}
            ])
        else:
            # Low temporal selectivity - filter by attribute first
            stages.extend([
                {"$match": attr_match},
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
            
        # Add lookup stage
        stages.extend([
            {
                "$lookup": {
                    "from": "Payloads",
                    "localField": "vref",
                    "foreignField": "vref",
                    "as": "payload_data"
                }
            },
            {"$unwind": "$payload_data"}
        ])
        
        return stages