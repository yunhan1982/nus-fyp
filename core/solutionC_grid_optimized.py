from typing import Dict, Any, List
from collections import defaultdict
import motor.motor_asyncio
from .solutionC import SolutionC
from .bitemporal_statistics import EnhancedGridBasedStatisticsCollector as GridBasedStatisticsCollector
from .optimization_config import OptimizationConfig, get_config
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class SolutionC_GridOptimized(SolutionC):
    """Grid-based optimized version of SolutionC with collection-level statistics optimization"""
    
    def __init__(self, config: OptimizationConfig = None) -> None:
        super().__init__()
        self.name = "SolutionC_GridOptimized"
        self.config = config if config else get_config()
        self.stats_collector = GridBasedStatisticsCollector(self.config)
        self.db = self.client[self.name]
        # Track collection-specific statistics
        self.collection_stats = {col: {} for col in self.collections}
        
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Enhanced insertion with statistics collection"""
        # Collect statistics from sample
        await self.stats_collector.add_samples(rectangles)
        
        # Perform original insertion
        await super().insert_rectangle_to_collections(rectangles, entity)
        
        # Update collection-specific statistics
        await self._update_collection_statistics(rectangles)
        
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Optimized query with collection prioritization based on statistics"""
        if not self.client:
            await self.connect()
            
        # Get selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt, vt, tt, tt)
        
        # Determine optimal collection order based on statistics
        collection_order = self._get_optimal_collection_order(["Name", "Age"], temporal_selectivity)
        
        # Build optimized aggregation pipeline
        pipeline = self._build_optimized_point_query_pipeline(
            name, age, tt, vt, entity, collection_order, temporal_selectivity
        )
        
        with Timer() as timer:
            # Execute on the most selective collection first
            primary_collection = collection_order[0]
            cursor = self.db[primary_collection].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Point query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Optimized range query with collection-level optimization"""
        if not self.client:
            await self.connect()
            
        # Get selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        
        # Determine optimal collection strategy
        collection_order = self._get_optimal_collection_order(["Name", "Age"], temporal_selectivity)
        
        # Build optimized pipeline
        pipeline = self._build_optimized_range_query_pipeline(
            name, age, vt_from, vt_to, tt_from, tt_to, entity, collection_order, temporal_selectivity
        )
        
        with Timer() as timer:
            primary_collection = collection_order[0]
            cursor = self.db[primary_collection].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Range query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Optimized attribute range query with single collection focus"""
        if not self.client:
            await self.connect()
            
        # Get selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        
        # Map attribute to collection
        collection_name = attribute_name.capitalize()
        if collection_name not in self.collections:
            collection_name = "Name"  # Fallback
            
        # Build optimized single-collection pipeline
        pipeline = self._build_optimized_attribute_pipeline(
            attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity
        )
        
        with Timer() as timer:
            cursor = self.db[collection_name].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        print(f"{self.name}: Attribute query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def _update_collection_statistics(self, rectangles: List[Rectangle]):
        """Update collection-specific statistics for optimization"""
        for rect in rectangles:
            for collection in self.collections:
                value = rect.data["payload"].get(collection.lower())
                if value is not None:
                    if collection not in self.collection_stats:
                        self.collection_stats[collection] = {"value_count": 0, "unique_values": set()}
                    
                    self.collection_stats[collection]["value_count"] += 1
                    self.collection_stats[collection]["unique_values"].add(str(value))
                    
    def _get_optimal_collection_order(self, target_collections, temporal_selectivity):
        """Determine optimal collection order based on statistics"""
        # Calculate selectivity for each collection
        collection_selectivity = {}
        
        for collection in target_collections:
            if collection in self.collection_stats:
                stats = self.collection_stats[collection]
                unique_ratio = len(stats.get("unique_values", set())) / max(stats.get("value_count", 1), 1)
                collection_selectivity[collection] = unique_ratio
            else:
                collection_selectivity[collection] = 0.5  # Default
                
        # Sort by selectivity (higher selectivity = more unique values = better for filtering)
        sorted_collections = sorted(target_collections, 
                                  key=lambda x: collection_selectivity[x], 
                                  reverse=True)
        
        return sorted_collections
        
    def _build_optimized_point_query_pipeline(self, name, age, tt, vt, entity, collection_order, temporal_selectivity):
        """Build optimized pipeline for point queries with collection prioritization"""
        primary_collection = collection_order[0]
        
        # Determine the value to search for based on primary collection
        search_value = name if primary_collection == "Name" else age
        
        stages = []
        
        # Optimize stage ordering based on temporal selectivity
        if temporal_selectivity < 0.1:  # High temporal selectivity
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
                        "value": str(search_value),
                        "entity": entity
                    }
                }
            ])
        else:
            # Low temporal selectivity - filter by value first
            stages.extend([
                {
                    "$match": {
                        "value": str(search_value),
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
            
        # Add lookup to other collections if needed
        if len(collection_order) > 1:
            secondary_collection = collection_order[1]
            secondary_value = age if secondary_collection == "Age" else name
            
            stages.append({
                "$lookup": {
                    "from": secondary_collection,
                    "let": {"eref": "$eref"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$eref", "$$eref"]},
                                        {"$eq": ["$value", str(secondary_value)]},
                                        {"$lte": ["$vt_from", vt]},
                                        {"$gt": ["$vt_to", vt]},
                                        {"$lte": ["$tt_from", tt]},
                                        {"$gt": ["$tt_to", tt]}
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "secondary_match"
                }
            })
            
            stages.append({
                "$match": {
                    "secondary_match": {"$ne": []}
                }
            })
            
        return stages
        
    def _build_optimized_range_query_pipeline(self, name, age, vt_from, vt_to, tt_from, tt_to, entity, collection_order, temporal_selectivity):
        """Build optimized pipeline for range queries"""
        primary_collection = collection_order[0]
        search_value = name if primary_collection == "Name" else age
        
        stages = []
        
        # Optimize based on temporal selectivity
        if temporal_selectivity < 0.2:  # High temporal selectivity
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
                        "value": str(search_value),
                        "entity": entity
                    }
                }
            ])
        else:
            # Low temporal selectivity
            stages.extend([
                {
                    "$match": {
                        "value": str(search_value),
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
            
        # Add secondary collection lookup if needed
        if len(collection_order) > 1:
            secondary_collection = collection_order[1]
            secondary_value = age if secondary_collection == "Age" else name
            
            stages.append({
                "$lookup": {
                    "from": secondary_collection,
                    "let": {"eref": "$eref"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$eref", "$$eref"]},
                                        {"$eq": ["$value", str(secondary_value)]},
                                        {"$lt": ["$vt_from", vt_to]},
                                        {"$gt": ["$vt_to", vt_from]},
                                        {"$lt": ["$tt_from", tt_to]},
                                        {"$gt": ["$tt_to", tt_from]}
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "secondary_match"
                }
            })
            
            stages.append({
                "$match": {
                    "secondary_match": {"$ne": []}
                }
            })
            
        return stages
        
    def _build_optimized_attribute_pipeline(self, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity):
        """Build optimized pipeline for single attribute queries"""
        stages = []
        
        if temporal_selectivity < 0.2:  # High temporal selectivity
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
                        "value": str(attribute_value),
                        "entity": entity
                    }
                }
            ])
        else:
            # Low temporal selectivity
            stages.extend([
                {
                    "$match": {
                        "value": str(attribute_value),
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
            
        return stages