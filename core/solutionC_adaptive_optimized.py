from typing import Dict, Any, List
from collections import defaultdict
import motor.motor_asyncio
from .solutionC import SolutionC
from .bitemporal_statistics import EnhancedAdaptiveStatisticsCollector
from .optimization_config import OptimizationConfig, get_config
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class SolutionC_AdaptiveOptimized(SolutionC):
    """Adaptive optimized version of SolutionC with enhanced collection-level optimization"""
    
    def __init__(self, config: OptimizationConfig = None) -> None:
        super().__init__()
        self.name = "SolutionC_AdaptiveOptimized"
        self.config = config if config else get_config()
        self.stats_collector = EnhancedAdaptiveStatisticsCollector(self.config)
        self.db = self.client[self.name]
        # Enhanced collection statistics with adaptive tracking
        self.collection_stats = {col: {
            "value_distribution": defaultdict(int),
            "temporal_patterns": [],
            "query_patterns": defaultdict(int),
            "selectivity_history": []
        } for col in self.collections}
        
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Enhanced insertion with adaptive statistics collection"""
        # Collect enhanced statistics
        await self.stats_collector.add_samples(rectangles)
        
        # Perform original insertion
        await super().insert_rectangle_to_collections(rectangles, entity)
        
        # Update adaptive collection statistics
        await self._update_adaptive_collection_statistics(rectangles)
        
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Adaptive optimized query with dynamic collection strategy"""
        if not self.client:
            await self.connect()
            
        # Get enhanced selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt, vt, tt, tt)
        locality_score = self.stats_collector.estimate_locality_score(vt, vt, tt, tt)
        
        # Record query pattern for adaptive learning
        query_signature = f"point_{name}_{age}"
        self._record_query_pattern(query_signature, ["Name", "Age"])
        
        # Determine adaptive collection strategy
        collection_strategy = self._get_adaptive_collection_strategy(
            ["Name", "Age"], temporal_selectivity, locality_score, query_signature
        )
        
        # Build adaptive aggregation pipeline
        pipeline = self._build_adaptive_point_query_pipeline(
            name, age, tt, vt, entity, collection_strategy, temporal_selectivity, locality_score
        )
        
        with Timer() as timer:
            # Execute with adaptive strategy
            primary_collection = collection_strategy["primary_collection"]
            cursor = self.db[primary_collection].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        # Update performance feedback
        self._update_performance_feedback(query_signature, timer.elapsed, len(results))
        
        print(f"{self.name}: Adaptive point query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Adaptive optimized range query with enhanced collection optimization"""
        if not self.client:
            await self.connect()
            
        # Get enhanced selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        locality_score = self.stats_collector.estimate_locality_score(vt_from, vt_to, tt_from, tt_to)
        
        # Record and analyze query pattern
        query_signature = f"range_{name}_{age}"
        self._record_query_pattern(query_signature, ["Name", "Age"])
        
        # Get adaptive strategy
        collection_strategy = self._get_adaptive_collection_strategy(
            ["Name", "Age"], temporal_selectivity, locality_score, query_signature
        )
        
        # Build adaptive pipeline
        pipeline = self._build_adaptive_range_query_pipeline(
            name, age, vt_from, vt_to, tt_from, tt_to, entity, collection_strategy, 
            temporal_selectivity, locality_score
        )
        
        with Timer() as timer:
            primary_collection = collection_strategy["primary_collection"]
            cursor = self.db[primary_collection].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        # Update performance feedback
        self._update_performance_feedback(query_signature, timer.elapsed, len(results))
        
        print(f"{self.name}: Adaptive range query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Adaptive optimized attribute range query with single collection focus"""
        if not self.client:
            await self.connect()
            
        # Get enhanced selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        locality_score = self.stats_collector.estimate_locality_score(vt_from, vt_to, tt_from, tt_to)
        
        # Map attribute to collection with adaptive selection
        collection_name = self._get_optimal_attribute_collection(attribute_name, attribute_value)
        
        # Record query pattern
        query_signature = f"attr_{attribute_name}_{attribute_value}"
        self._record_query_pattern(query_signature, [collection_name])
        
        # Build adaptive single-collection pipeline
        pipeline = self._build_adaptive_attribute_pipeline(
            attribute_value, vt_from, vt_to, tt_from, tt_to, entity, 
            temporal_selectivity, locality_score
        )
        
        with Timer() as timer:
            cursor = self.db[collection_name].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        # Update performance feedback
        self._update_performance_feedback(query_signature, timer.elapsed, len(results))
        
        print(f"{self.name}: Adaptive attribute query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def _update_adaptive_collection_statistics(self, rectangles: List[Rectangle]):
        """Update adaptive collection statistics with enhanced tracking"""
        for rect in rectangles:
            for collection in self.collections:
                value = rect.data["payload"].get(collection.lower())
                if value is not None:
                    stats = self.collection_stats[collection]
                    
                    # Update value distribution
                    stats["value_distribution"][str(value)] += 1
                    
                    # Track temporal patterns
                    temporal_point = {
                        "vt": (rect.vt_from + rect.vt_to) / 2,
                        "tt": (rect.tt_from + rect.tt_to) / 2,
                        "value": str(value)
                    }
                    stats["temporal_patterns"].append(temporal_point)
                    
                    # Limit history size for memory efficiency
                    if len(stats["temporal_patterns"]) > 1000:
                        stats["temporal_patterns"] = stats["temporal_patterns"][-500:]
                        
    def _record_query_pattern(self, query_signature: str, collections: List[str]):
        """Record query patterns for adaptive learning"""
        for collection in collections:
            if collection in self.collection_stats:
                self.collection_stats[collection]["query_patterns"][query_signature] += 1
                
    def _get_adaptive_collection_strategy(self, target_collections, temporal_selectivity, locality_score, query_signature):
        """Determine adaptive collection strategy based on multiple factors"""
        collection_scores = {}
        
        for collection in target_collections:
            if collection in self.collection_stats:
                stats = self.collection_stats[collection]
                
                # Calculate base selectivity
                total_values = sum(stats["value_distribution"].values())
                unique_values = len(stats["value_distribution"])
                base_selectivity = unique_values / max(total_values, 1)
                
                # Factor in query pattern frequency
                query_frequency = stats["query_patterns"].get(query_signature, 0)
                frequency_bonus = min(query_frequency * 0.1, 0.5)  # Cap at 0.5
                
                # Factor in temporal locality
                locality_bonus = locality_score * 0.3
                
                # Calculate composite score
                composite_score = base_selectivity + frequency_bonus + locality_bonus
                collection_scores[collection] = composite_score
            else:
                collection_scores[collection] = 0.5  # Default
                
        # Determine strategy
        primary_collection = max(target_collections, key=lambda x: collection_scores[x])
        
        strategy = {
            "primary_collection": primary_collection,
            "use_parallel_lookup": temporal_selectivity > 0.3,  # Use parallel for low temporal selectivity
            "optimize_for_locality": locality_score > 0.7,
            "collection_scores": collection_scores
        }
        
        return strategy
        
    def _get_optimal_attribute_collection(self, attribute_name: str, attribute_value: Any) -> str:
        """Get optimal collection for attribute with adaptive selection"""
        collection_name = attribute_name.capitalize()
        
        # Check if collection exists and has good statistics
        if collection_name in self.collections and collection_name in self.collection_stats:
            stats = self.collection_stats[collection_name]
            value_frequency = stats["value_distribution"].get(str(attribute_value), 0)
            
            # If value is rare, this collection is good for filtering
            if value_frequency > 0:
                return collection_name
                
        # Fallback to most selective collection
        best_collection = "Name"  # Default
        best_selectivity = 0
        
        for collection in self.collections:
            if collection in self.collection_stats:
                stats = self.collection_stats[collection]
                total_values = sum(stats["value_distribution"].values())
                unique_values = len(stats["value_distribution"])
                selectivity = unique_values / max(total_values, 1)
                
                if selectivity > best_selectivity:
                    best_selectivity = selectivity
                    best_collection = collection
                    
        return best_collection
        
    def _build_adaptive_point_query_pipeline(self, name, age, tt, vt, entity, strategy, temporal_selectivity, locality_score):
        """Build adaptive pipeline for point queries with enhanced optimization"""
        primary_collection = strategy["primary_collection"]
        search_value = name if primary_collection == "Name" else age
        
        stages = []
        
        # Adaptive stage ordering based on multiple factors
        if temporal_selectivity < 0.1 or strategy["optimize_for_locality"]:
            # High temporal selectivity or high locality - temporal filter first
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
            # Low temporal selectivity - value filter first
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
            
        # Add adaptive secondary collection lookup
        secondary_collections = [col for col in ["Name", "Age"] if col != primary_collection]
        
        if secondary_collections and strategy["use_parallel_lookup"]:
            secondary_collection = secondary_collections[0]
            secondary_value = age if secondary_collection == "Age" else name
            
            # Enhanced lookup with adaptive pipeline
            lookup_pipeline = [
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
            ]
            
            # Add projection for efficiency if high locality
            if strategy["optimize_for_locality"]:
                lookup_pipeline.append({
                    "$project": {
                        "eref": 1,
                        "value": 1,
                        "vt_from": 1,
                        "vt_to": 1
                    }
                })
                
            stages.append({
                "$lookup": {
                    "from": secondary_collection,
                    "let": {"eref": "$eref"},
                    "pipeline": lookup_pipeline,
                    "as": "secondary_match"
                }
            })
            
            stages.append({
                "$match": {
                    "secondary_match": {"$ne": []}
                }
            })
            
        return stages
        
    def _build_adaptive_range_query_pipeline(self, name, age, vt_from, vt_to, tt_from, tt_to, entity, strategy, temporal_selectivity, locality_score):
        """Build adaptive pipeline for range queries with enhanced optimization"""
        primary_collection = strategy["primary_collection"]
        search_value = name if primary_collection == "Name" else age
        
        stages = []
        
        # Adaptive optimization based on range characteristics
        temporal_range_size = (vt_to - vt_from) * (tt_to - tt_from)
        
        if temporal_selectivity < 0.2 or temporal_range_size < 100:  # Small range or high selectivity
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
            # Large range or low selectivity
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
            
        # Add adaptive secondary collection processing
        secondary_collections = [col for col in ["Name", "Age"] if col != primary_collection]
        
        if secondary_collections and strategy["use_parallel_lookup"]:
            secondary_collection = secondary_collections[0]
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
        
    def _build_adaptive_attribute_pipeline(self, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, temporal_selectivity, locality_score):
        """Build adaptive pipeline for single attribute queries with enhanced optimization"""
        stages = []
        
        # Adaptive optimization based on selectivity and locality
        if temporal_selectivity < 0.2 or locality_score > 0.7:
            # High temporal selectivity or high locality
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
            # Low temporal selectivity and locality
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
            
        # Add result limiting for large result sets
        if temporal_selectivity > 0.5:  # Low selectivity might return many results
            stages.append({
                "$limit": 10000  # Reasonable limit for performance
            })
            
        return stages
        
    def _update_performance_feedback(self, query_signature: str, execution_time: float, result_count: int):
        """Update performance feedback for adaptive learning"""
        # Update selectivity history for relevant collections
        for collection in self.collection_stats:
            if query_signature in self.collection_stats[collection]["query_patterns"]:
                selectivity_data = {
                    "query": query_signature,
                    "execution_time": execution_time,
                    "result_count": result_count,
                    "selectivity": result_count / max(execution_time * 1000, 1)  # Results per ms
                }
                
                self.collection_stats[collection]["selectivity_history"].append(selectivity_data)
                
                # Limit history size
                if len(self.collection_stats[collection]["selectivity_history"]) > 100:
                    self.collection_stats[collection]["selectivity_history"] = \
                        self.collection_stats[collection]["selectivity_history"][-50:]