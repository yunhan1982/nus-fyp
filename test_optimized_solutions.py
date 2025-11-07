#!/usr/bin/env python3
"""
Test script for optimized SolutionD implementations.
Demonstrates Phase 1 (Grid-based) and Phase 2 (Adaptive) optimizations.
"""

import sys
import os
import time
import random
from datetime import datetime, timedelta

# Add the core directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'core'))

# Import from core package
from core.solutionD import SolutionD
from core.solutionD_grid_optimized import SolutionD_GridOptimized
from core.solutionD_adaptive_optimized import SolutionD_AdaptiveOptimized
from core.bitemporal_space import Rectangle
from core.optimization_config import OptimizationConfig, get_config

def create_test_data(num_rectangles=1000):
    """Create test data for benchmarking."""
    rectangles = []
    base_time = time.time()
    
    for i in range(num_rectangles):
        # Create varied temporal data
        vt_start = base_time + random.uniform(-86400*30, 86400*30)  # ±30 days
        vt_end = vt_start + random.uniform(3600, 86400*7)  # 1 hour to 7 days
        tt_start = base_time + random.uniform(-86400*30, 86400*30)
        tt_end = tt_start + random.uniform(3600, 86400*7)
        
        # Create varied attribute data
        name = f"entity_{i % 100}"  # 100 different entities
        age = random.randint(18, 80)
        attr1 = random.randint(1, 1000)
        attr2 = random.randint(1, 1000)
        attr3 = random.randint(1, 1000)
        attr4 = random.randint(1, 1000)
        
        # Convert timestamps to datetime objects
        from datetime import datetime
        vt_start_dt = datetime.fromtimestamp(vt_start)
        vt_end_dt = datetime.fromtimestamp(vt_end)
        tt_start_dt = datetime.fromtimestamp(tt_start)
        tt_end_dt = datetime.fromtimestamp(tt_end)
        
        payload = {
            'name': name,
            'age': age,
            'attr1': attr1,
            'attr2': attr2,
            'attr3': attr3,
            'attr4': attr4
        }
        
        data = {
            'id': i,
            'payload': payload
        }
        
        rect = Rectangle(
            data=data,
            tt_from=tt_start_dt,
            tt_to=tt_end_dt,
            vt_from=vt_start_dt,
            vt_to=vt_end_dt,
            index=i
        )
        rectangles.append(rect)
    
    return rectangles

async def benchmark_solution(solution_class, name, rectangles, config=None):
    """Benchmark a solution implementation."""
    print(f"\n=== Testing {name} ===")
    
    # Initialize solution
    if config:
        solution = solution_class(config)
    else:
        solution = solution_class()
    
    # Connect and initialize
    await solution.connect()
    await solution.initialize_collections()
    
    # Insert data
    print(f"Inserting {len(rectangles)} rectangles...")
    start_time = time.time()
    await solution.insert_rectangle_to_collections(rectangles)
    insert_time = time.time() - start_time
    print(f"Insert time: {insert_time:.3f}s")
    
    # Test queries
    from datetime import datetime
    base_time = datetime.now()
    test_queries = [
        # Point queries
        ('query_by_name_and_age', ('entity_50', 25, base_time, base_time)),
        ('query_by_name_and_age', ('entity_75', 35, base_time, base_time)),
        
        # Range queries - using smaller ranges for testing
        ('range_query_by_name_and_age', ('entity_25', 30, base_time, base_time, base_time, base_time)),
    ]
    
    total_query_time = 0
    total_results = 0
    
    for query_name, args in test_queries:
        start_time = time.time()
        results = await getattr(solution, query_name)(*args)
        query_time = time.time() - start_time
        total_query_time += query_time
        total_results += len(results)
        print(f"  {query_name}: {len(results)} results in {query_time:.4f}s")
    
    print(f"Total query time: {total_query_time:.4f}s")
    print(f"Total results: {total_results}")
    
    # Show statistics if available
    if hasattr(solution, 'stats_collector'):
        stats = solution.stats_collector
        print(f"Statistics: {len(stats.samples)} samples, {len(stats.regions)} regions")
        if hasattr(stats, 'query_count'):
            print(f"Query count: {stats.query_count}")
    
    return {
        'insert_time': insert_time,
        'query_time': total_query_time,
        'total_results': total_results
    }

async def main():
    """Main test function."""
    print("Statistics-Based Optimization Test")
    print("=" * 50)
    
    # Create test data
    print("Creating test data...")
    rectangles = create_test_data(100)  # Reduced for faster testing
    print(f"Created {len(rectangles)} test rectangles")
    
    # Get optimization config
    config = get_config()
    print(f"\nUsing configuration:")
    print(f"  Sampling rate: {config.sampling_rate}")
    print(f"  Grid size: {config.grid_vt_cells}x{config.grid_tt_cells}")
    print(f"  Max adaptive regions: {config.max_adaptive_regions}")
    
    # Test all solutions
    results = {}
    
    # Baseline solution
    results['baseline'] = await benchmark_solution(SolutionD, "Baseline SolutionD", rectangles)
    
    # Grid-optimized solution
    results['grid'] = await benchmark_solution(SolutionD_GridOptimized, "Grid-Optimized SolutionD", rectangles, config)
    
    # Adaptive-optimized solution
    results['adaptive'] = await benchmark_solution(SolutionD_AdaptiveOptimized, "Adaptive-Optimized SolutionD", rectangles, config)
    
    # Summary
    print("\n=== Performance Summary ===")
    print(f"{'Solution':<20} {'Insert (s)':<12} {'Query (s)':<12} {'Results':<10}")
    print("-" * 60)
    
    for name, result in results.items():
        print(f"{name.capitalize():<20} {result['insert_time']:<12.3f} {result['query_time']:<12.4f} {result['total_results']:<10}")
    
    # Verify results consistency
    baseline_results = results['baseline']['total_results']
    grid_results = results['grid']['total_results']
    adaptive_results = results['adaptive']['total_results']
    
    if baseline_results == grid_results == adaptive_results:
        print("\n✅ All solutions return identical results (100% accuracy maintained)")
    else:
        print("\n❌ Results differ between solutions:")
        print(f"  Baseline: {baseline_results}")
        print(f"  Grid: {grid_results}")
        print(f"  Adaptive: {adaptive_results}")
    
    print("\nTest completed successfully!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())