from typing import Dict, Any, List
from collections import defaultdict
import motor.motor_asyncio
from .solutionA2 import SolutionA2
from .bitemporal_statistics import EnhancedAdaptiveStatisticsCollector
from .optimization_config import OptimizationConfig, get_config
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class SolutionA2_AdaptiveOptimized(SolutionA2):
    """Adaptive optimized version of SolutionA2 with enhanced aggregation pipeline optimization"""
    
    def __init__(self, config: OptimizationConfig = None) -> None:
        super().__init__()
        self.name = "SolutionA2_AdaptiveOptimized"
        self.config = config if config else get_config()
        self.stats_collector = EnhancedAdaptiveStatisticsCollector(self.config)
        self.db = self.client[self.name]
        # Enhanced field statistics with adaptive tracking
        self.field_stats = {
            "name": {
                "selectivity_history": [],
                "value_distribution": defaultdict(int),
                "query_patterns": defaultdict(int),
                "performance_metrics": []
            },
            "age": {
                "selectivity_history": [],
                "value_distribution": defaultdict(int),
                "query_patterns": defaultdict(int),
                "performance_metrics": []
            },
            "temporal": {
                "selectivity_history": [],
                "range_patterns": [],
                "query_patterns": defaultdict(int),
                "performance_metrics": []
            }
        }
        
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Enhanced insertion with adaptive statistics collection"""
        # Collect enhanced statistics
        await self.stats_collector.add_samples(rectangles)
        
        # Perform original insertion
        await super().insert_rectangle_to_collections(rectangles, entity)
        
        # Update adaptive field statistics
        await self._update_adaptive_field_statistics(rectangles)
        
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Adaptive optimized query with dynamic pipeline optimization"""
        if not self.client:
            await self.connect()
            
        # Get enhanced selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt, vt, tt, tt)
        locality_score = self.stats_collector.estimate_locality_score(vt, vt, tt, tt)
        
        # Record query pattern for adaptive learning
        query_signature = f"point_{name}_{age}"
        self._record_query_pattern(query_signature, ["name", "age", "temporal"])
        
        # Determine adaptive pipeline strategy
        pipeline_strategy = self._get_adaptive_pipeline_strategy(
            ["name", "age"], temporal_selectivity, locality_score, query_signature
        )
        
        # Build adaptive aggregation pipeline
        pipeline = self._build_adaptive_point_query_pipeline(
            name, age, tt, vt, entity, pipeline_strategy, temporal_selectivity, locality_score
        )
        
        with Timer() as timer:
            cursor = self.db[entity].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        # Update performance feedback for adaptive learning
        self._update_performance_feedback(query_signature, timer.elapsed, len(results), pipeline_strategy)
        
        print(f"{self.name}: Adaptive point query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Adaptive optimized range query with enhanced pipeline optimization"""
        if not self.client:
            await self.connect()
            
        # Get enhanced selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        locality_score = self.stats_collector.estimate_locality_score(vt_from, vt_to, tt_from, tt_to)
        
        # Record and analyze query pattern
        query_signature = f"range_{name}_{age}"
        self._record_query_pattern(query_signature, ["name", "age", "temporal"])
        
        # Get adaptive strategy with range-specific considerations
        pipeline_strategy = self._get_adaptive_range_pipeline_strategy(
            ["name", "age"], temporal_selectivity, locality_score, query_signature, 
            vt_from, vt_to, tt_from, tt_to
        )
        
        # Build adaptive pipeline
        pipeline = self._build_adaptive_range_query_pipeline(
            name, age, vt_from, vt_to, tt_from, tt_to, entity, pipeline_strategy, 
            temporal_selectivity, locality_score
        )
        
        with Timer() as timer:
            cursor = self.db[entity].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        # Update performance feedback
        self._update_performance_feedback(query_signature, timer.elapsed, len(results), pipeline_strategy)
        
        print(f"{self.name}: Adaptive range query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Adaptive optimized attribute range query with field-specific optimization"""
        if not self.client:
            await self.connect()
            
        # Get enhanced selectivity estimates
        temporal_selectivity = self.stats_collector.estimate_temporal_selectivity(vt_from, vt_to, tt_from, tt_to)
        locality_score = self.stats_collector.estimate_locality_score(vt_from, vt_to, tt_from, tt_to)
        
        # Record query pattern for the specific attribute
        query_signature = f"attr_{attribute_name}_{attribute_value}"
        self._record_query_pattern(query_signature, [attribute_name.lower(), "temporal"])
        
        # Get adaptive strategy for single attribute
        pipeline_strategy = self._get_adaptive_attribute_pipeline_strategy(
            attribute_name, attribute_value, temporal_selectivity, locality_score, query_signature
        )
        
        # Build adaptive single-attribute pipeline
        pipeline = self._build_adaptive_attribute_pipeline(
            attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, 
            pipeline_strategy, temporal_selectivity, locality_score
        )
        
        with Timer() as timer:
            cursor = self.db[entity].aggregate(pipeline)
            results = await cursor.to_list(length=None)
            
        # Update performance feedback
        self._update_performance_feedback(query_signature, timer.elapsed, len(results), pipeline_strategy)
        
        print(f"{self.name}: Adaptive attribute query completed in {timer.elapsed:.4f}s with {len(results)} results")
        return results
        
    async def _update_adaptive_field_statistics(self, rectangles: List[Rectangle]):
        """Update adaptive field statistics with enhanced tracking"""
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
                
            # Update temporal statistics
            temporal_point = {
                "vt_center": (rect.vt_from + rect.vt_to) / 2,
                "tt_center": (rect.tt_from + rect.tt_to) / 2,
                "vt_range": rect.vt_to - rect.vt_from,
                "tt_range": rect.tt_to - rect.tt_from
            }
            self.field_stats["temporal"]["range_patterns"].append(temporal_point)
            
            # Limit history size for memory efficiency
            if len(self.field_stats["temporal"]["range_patterns"]) > 1000:
                self.field_stats["temporal"]["range_patterns"] = \
                    self.field_stats["temporal"]["range_patterns"][-500:]
                    
    def _record_query_pattern(self, query_signature: str, fields: List[str]):
        """Record query patterns for adaptive learning"""
        for field in fields:
            if field in self.field_stats:
                self.field_stats[field]["query_patterns"][query_signature] += 1
                
    def _get_adaptive_pipeline_strategy(self, target_fields, temporal_selectivity, locality_score, query_signature):
        """Determine adaptive pipeline strategy based on multiple factors"""
        field_scores = {}
        
        for field in target_fields:
            if field in self.field_stats:
                stats = self.field_stats[field]
                
                # Calculate base selectivity from value distribution
                total_values = sum(stats["value_distribution"].values())
                unique_values = len(stats["value_distribution"])
                base_selectivity = unique_values / max(total_values, 1)
                
                # Factor in query pattern frequency
                query_frequency = stats["query_patterns"].get(query_signature, 0)
                frequency_bonus = min(query_frequency * 0.05, 0.3)  # Cap frequency bonus
                
                # Factor in historical performance
                performance_bonus = self._calculate_performance_bonus(field, query_signature)
                
                # Calculate composite score
                composite_score = base_selectivity + frequency_bonus + performance_bonus
                field_scores[field] = composite_score
            else:
                field_scores[field] = 0.5  # Default score
                
        # Add temporal considerations
        temporal_score = temporal_selectivity + (locality_score * 0.2)
        field_scores["temporal"] = temporal_score
        
        # Determine optimal strategy
        sorted_fields = sorted(target_fields + ["temporal"], 
                             key=lambda x: field_scores.get(x, 0.5), 
                             reverse=True)
        
        strategy = {
            "field_order": sorted_fields,
            "use_compound_index": temporal_selectivity > 0.3,
            "use_parallel_stages": locality_score > 0.6,
            "optimize_for_locality": locality_score > 0.7,
            "field_scores": field_scores,
            "primary_field": sorted_fields[0]
        }
        
        return strategy
        
    def _get_adaptive_range_pipeline_strategy(self, target_fields, temporal_selectivity, locality_score, query_signature, vt_from, vt_to, tt_from, tt_to):
        """Get adaptive strategy with range-specific considerations"""
        base_strategy = self._get_adaptive_pipeline_strategy(target_fields, temporal_selectivity, locality_score, query_signature)
        
        # Calculate range characteristics
        temporal_range_size = (vt_to - vt_from) * (tt_to - tt_from)
        
        # Enhance strategy with range-specific optimizations
        base_strategy.update({
            "temporal_range_size": temporal_range_size,
            "use_range_optimization": temporal_range_size < 100,
            "use_staged_filtering": temporal_range_size > 1000,
            "prefer_temporal_first": temporal_selectivity < 0.15 and temporal_range_size < 200
        })
        
        return base_strategy
        
    def _get_adaptive_attribute_pipeline_strategy(self, attribute_name, attribute_value, temporal_selectivity, locality_score, query_signature):
        """Get adaptive strategy for single attribute queries"""
        field_key = attribute_name.lower()
        
        # Calculate attribute-specific selectivity
        attribute_selectivity = 0.5  # Default
        if field_key in self.field_stats:
            stats = self.field_stats[field_key]
            value_frequency = stats["value_distribution"].get(str(attribute_value), 0)
            total_values = sum(stats["value_distribution"].values())
            if total_values > 0:
                attribute_selectivity = 1 - (value_frequency / total_values)  # Higher for rarer values
                
        # Determine strategy
        strategy = {
            "attribute_selectivity": attribute_selectivity,
            "temporal_selectivity": temporal_selectivity,
            "use_attribute_first": attribute_selectivity > temporal_selectivity,
            "use_compound_index": attribute_selectivity > 0.3 and temporal_selectivity > 0.3,
            "optimize_for_locality": locality_score > 0.7,
            "limit_results": attribute_selectivity < 0.2 and temporal_selectivity < 0.2
        }
        
        return strategy
        
    def _calculate_performance_bonus(self, field: str, query_signature: str) -> float:
        """Calculate performance bonus based on historical performance"""
        if field not in self.field_stats:
            return 0.0
            
        performance_metrics = self.field_stats[field]["performance_metrics"]
        relevant_metrics = [m for m in performance_metrics if m["query"] == query_signature]
        
        if not relevant_metrics:
            return 0.0
            
        # Calculate average performance (lower execution time = higher bonus)
        avg_time = sum(m["execution_time"] for m in relevant_metrics) / len(relevant_metrics)
        performance_bonus = max(0.1 - avg_time, 0) * 0.5  # Cap at 0.05
        
        return performance_bonus
        
    def _build_adaptive_point_query_pipeline(self, name, age, tt, vt, entity, strategy, temporal_selectivity, locality_score):
        """Build adaptive aggregation pipeline for point queries"""
        stages = []
        field_order = strategy["field_order"]
        
        # Add index hint based on primary field
        if strategy["use_compound_index"]:
            primary_field = strategy["primary_field"]
            if primary_field == "name":
                stages.append({
                    "$hint": {
                        "name": 1,
                        "entity": 1,
                        "vt_from": 1,
                        "vt_to": 1
                    }
                })
            elif primary_field == "age":
                stages.append({
                    "$hint": {
                        "age": 1,
                        "vt_from": 1,
                        "vt_to": 1
                    }
                })
            elif primary_field == "temporal":
                stages.append({
                    "$hint": {
                        "vt_from": 1,
                        "vt_to": 1,
                        "tt_from": 1,
                        "tt_to": 1
                    }
                })
                
        # Build match stages in optimal order
        if strategy["use_parallel_stages"] and locality_score > 0.7:
            # Use parallel filtering for high locality
            combined_match = {"entity": entity}
            
            for field in field_order:
                if field == "name":
                    combined_match["name"] = str(name)
                elif field == "age":
                    combined_match["age"] = str(age)
                elif field == "temporal":
                    combined_match.update({
                        "vt_from": {"$lte": vt},
                        "vt_to": {"$gt": vt},
                        "tt_from": {"$lte": tt},
                        "tt_to": {"$gt": tt}
                    })
                    
            stages.append({"$match": combined_match})
        else:
            # Use sequential filtering
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
                    
        # Add projection optimization for high locality
        if strategy["optimize_for_locality"]:
            stages.append({
                "$project": {
                    "_id": 1,
                    "name": 1,
                    "age": 1,
                    "entity": 1,
                    "vt_from": 1,
                    "vt_to": 1,
                    "tt_from": 1,
                    "tt_to": 1
                }
            })
            
        return stages
        
    def _build_adaptive_range_query_pipeline(self, name, age, vt_from, vt_to, tt_from, tt_to, entity, strategy, temporal_selectivity, locality_score):
        """Build adaptive aggregation pipeline for range queries"""
        stages = []
        field_order = strategy["field_order"]
        
        # Add adaptive index hint
        if strategy["use_compound_index"]:
            primary_field = strategy["primary_field"]
            if primary_field == "temporal" and strategy["prefer_temporal_first"]:
                stages.append({
                    "$hint": {
                        "vt_from": 1,
                        "vt_to": 1,
                        "tt_from": 1,
                        "tt_to": 1
                    }
                })
            elif primary_field == "name":
                stages.append({
                    "$hint": {
                        "name": 1,
                        "entity": 1,
                        "vt_from": 1
                    }
                })
                
        # Build stages based on range characteristics
        if strategy["use_staged_filtering"]:
            # Large range - use staged filtering
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
                    # Split temporal filtering into stages for large ranges
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
        else:
            # Small/medium range - use optimized filtering
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
                            "$and": [
                                {"vt_from": {"$lt": vt_to}},
                                {"vt_to": {"$gt": vt_from}},
                                {"tt_from": {"$lt": tt_to}},
                                {"tt_to": {"$gt": tt_from}}
                            ]
                        }
                    })
                    
        return stages
        
    def _build_adaptive_attribute_pipeline(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity, strategy, temporal_selectivity, locality_score):
        """Build adaptive pipeline for single attribute queries"""
        stages = []
        
        # Add appropriate index hint
        if strategy["use_compound_index"]:
            if strategy["use_attribute_first"]:
                stages.append({
                    "$hint": {
                        attribute_name.lower(): 1,
                        "entity": 1,
                        "vt_from": 1
                    }
                })
            else:
                stages.append({
                    "$hint": {
                        "vt_from": 1,
                        "vt_to": 1,
                        attribute_name.lower(): 1
                    }
                })
                
        # Build stages in optimal order
        if strategy["use_attribute_first"]:
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
        else:
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
            
        # Add result limiting if needed
        if strategy["limit_results"]:
            stages.append({
                "$limit": 10000
            })
            
        # Add projection optimization for high locality
        if strategy["optimize_for_locality"]:
            stages.append({
                "$project": {
                    "_id": 1,
                    attribute_name.lower(): 1,
                    "entity": 1,
                    "vt_from": 1,
                    "vt_to": 1,
                    "tt_from": 1,
                    "tt_to": 1
                }
            })
            
        return stages
        
    def _update_performance_feedback(self, query_signature: str, execution_time: float, result_count: int, strategy: Dict[str, Any]):
        """Update performance feedback for adaptive learning"""
        performance_data = {
            "query": query_signature,
            "execution_time": execution_time,
            "result_count": result_count,
            "strategy_used": strategy["primary_field"],
            "selectivity": result_count / max(execution_time * 1000, 1)  # Results per ms
        }
        
        # Update performance metrics for relevant fields
        for field in ["name", "age", "temporal"]:
            if field in self.field_stats:
                self.field_stats[field]["performance_metrics"].append(performance_data)
                
                # Limit history size
                if len(self.field_stats[field]["performance_metrics"]) > 100:
                    self.field_stats[field]["performance_metrics"] = \
                        self.field_stats[field]["performance_metrics"][-50:]
                        
        # Update selectivity history
        selectivity_data = {
            "query": query_signature,
            "temporal_selectivity": strategy.get("field_scores", {}).get("temporal", 0.5),
            "execution_time": execution_time,
            "result_count": result_count
        }
        
        for field in ["name", "age", "temporal"]:
            if field in self.field_stats:
                self.field_stats[field]["selectivity_history"].append(selectivity_data)
                
                # Limit history size
                if len(self.field_stats[field]["selectivity_history"]) > 100:
                    self.field_stats[field]["selectivity_history"] = \
                        self.field_stats[field]["selectivity_history"][-50:]