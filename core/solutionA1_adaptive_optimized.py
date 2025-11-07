from typing import Dict, Any, List
import motor.motor_asyncio
from .solutionA1 import SolutionA1
from .bitemporal_statistics import EnhancedAdaptiveStatisticsCollector as AdaptiveStatisticsCollector
from .optimization_config import OptimizationConfig, get_config
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class SolutionA1_AdaptiveOptimized(SolutionA1):
    """Adaptive optimized version of SolutionA1 with dynamic query pattern learning"""
    
    def __init__(self, config: OptimizationConfig = None) -> None:
        super().__init__()
        self.name = "SolutionA1_AdaptiveOptimized"
        self.config = config if config else get_config()
        self.stats_collector = AdaptiveStatisticsCollector(self.config)
        self.db = self.client[self.name]
        
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Enhanced insertion with adaptive statistics collection"""
        # Collect statistics and adapt regions
        await self.stats_collector.add_samples(rectangles)
        
        # Perform original insertion
        await super().insert_rectangle_to_collections(rectangles, entity)
        
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Adaptive optimized query with dynamic pattern learning"""
        if not self.client:
            await self.connect()
            
        # Record query pattern and get adaptive optimization
        query_region = (vt, vt, tt, tt)
        await self.stats_collector.record_query_pattern(query_region)
        
        # Get adaptive selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt, vt, tt, tt)
        locality_score = self.stats_collector.get_locality_score(vt, vt, tt, tt)
        
        # Build adaptively optimized pipeline
        pipeline = self._build_adaptive_pipeline_point_query(
            name, age, tt, vt, entity, temporal_selectivity, locality_score
        )
        
        with Timer() as timer:
            cursor = self.db.Index.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Adaptive point query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Adaptive optimized range query with pattern learning"""
        if not self.client:
            await self.connect()
            
        # Record query pattern
        query_region = (vt_from, vt_to, tt_from, tt_to)
        await self.stats_collector.record_query_pattern(query_region)
        
        # Get adaptive estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        locality_score = self.stats_collector.get_locality_score(vt_from, vt_to, tt_from, tt_to)
        
        # Build adaptive pipeline
        pipeline = self._build_adaptive_pipeline_range_query(
            name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity, locality_score
        )
        
        with Timer() as timer:
            cursor = self.db.Index.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Adaptive range query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Adaptive optimized attribute range query"""
        if not self.client:
            await self.connect()
            
        # Record query pattern
        query_region = (vt_from, vt_to, tt_from, tt_to)
        await self.stats_collector.record_query_pattern(query_region)
        
        # Get adaptive estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        locality_score = self.stats_collector.get_locality_score(vt_from, vt_to, tt_from, tt_to)
        
        # Build adaptive pipeline
        pipeline = self._build_adaptive_attribute_pipeline(
            attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, 
            temporal_selectivity, locality_score
        )
        
        with Timer() as timer:
            cursor = self.db.Index.aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Adaptive attribute query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    def _build_adaptive_pipeline_point_query(self, name, age, tt, vt, entity, temporal_selectivity, locality_score):
        """Build adaptively optimized pipeline for point queries"""
        stages = []
        
        # Use locality score to determine optimization strategy
        if locality_score > 0.7 and temporal_selectivity < 0.05:
            # High locality + high temporal selectivity: aggressive temporal filtering
            stages.extend([
                {
                    "$match": {
                        "vt_from": {"$lte": vt},
                        "vt_to": {"$gt": vt}
                    }
                },
                {
                    "$match": {
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
        elif locality_score < 0.3:
            # Low locality: prioritize attribute filtering
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
        else:
            # Balanced approach
            stages.append({
                "$match": {
                    "name": name,
                    "age": age,
                    "entity": entity,
                    "vt_from": {"$lte": vt},
                    "vt_to": {"$gt": vt},
                    "tt_from": {"$lte": tt},
                    "tt_to": {"$gt": tt}
                }
            })
            
        # Add lookup with potential optimization hints
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
        
    def _build_adaptive_pipeline_range_query(self, name, age, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity, locality_score):
        """Build adaptively optimized pipeline for range queries"""
        stages = []
        
        # Adaptive strategy based on locality and selectivity
        if locality_score > 0.8 and temporal_selectivity < 0.1:
            # Very high locality: use compound temporal filter first
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
                        "name": name,
                        "age": age,
                        "entity": entity
                    }
                }
            ])
        elif locality_score < 0.2 or temporal_selectivity > 0.5:
            # Low locality or low temporal selectivity: attribute-first strategy
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
            # Adaptive middle ground: separate VT and TT filtering
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
                        "vt_from": {"$lt": vt_to},
                        "vt_to": {"$gt": vt_from}
                    }
                },
                {
                    "$match": {
                        "tt_from": {"$lt": tt_to},
                        "tt_to": {"$gt": tt_from}
                    }
                }
            ])
            
        # Add lookup stages
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
        
    def _build_adaptive_attribute_pipeline(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity, locality_score):
        """Build adaptively optimized pipeline for attribute queries"""
        stages = []
        
        # Create attribute match condition
        attr_match = {attribute_name: attribute_value, "entity": entity}
        
        # Adaptive strategy for attribute queries
        if locality_score > 0.6 and temporal_selectivity < 0.15:
            # High locality: temporal-first approach
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
            # Lower locality or selectivity: attribute-first approach
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
            
        # Add lookup stages
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