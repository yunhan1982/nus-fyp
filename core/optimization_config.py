from dataclasses import dataclass
from typing import Dict, Any, List
import time

@dataclass
class OptimizationConfig:
    """Configuration for bitemporal statistics-based optimization."""
    
    # Sampling configuration
    sampling_rate: float = 0.01  # 1% sampling rate
    min_sample_size: int = 1000  # Minimum samples for reliable statistics
    max_sample_size: int = 100000  # Maximum samples to prevent memory issues
    
    # Temporal weighting configuration
    temporal_decay_factor: float = 0.95  # Exponential decay for older data
    recent_time_boost: float = 2.0  # Boost factor for recent temporal regions
    current_time_window_hours: int = 24  # Hours to consider as "recent"
    
    # Statistics update configuration
    batch_update_interval_seconds: int = 300  # 5 minutes
    force_update_threshold: float = 0.1  # Force update if 10% data change
    statistics_retention_days: int = 30  # Keep statistics for 30 days
    
    # Grid-based optimization (Phase 1)
    grid_vt_cells: int = 10  # Number of VT dimension cells
    grid_tt_cells: int = 10  # Number of TT dimension cells
    min_cell_samples: int = 50  # Minimum samples per cell
    
    # Adaptive optimization (Phase 2)
    adaptive_density_threshold: float = 0.05  # Threshold for region splitting
    max_adaptive_regions: int = 100  # Maximum number of adaptive regions
    region_merge_threshold: float = 0.02  # Threshold for region merging
    
    # Query optimization
    selectivity_threshold: float = 0.1  # Threshold for high selectivity
    max_predicate_reorder_depth: int = 5  # Maximum predicates to reorder
    enable_index_hints: bool = True  # Enable database index hints
    
    # Performance monitoring
    enable_performance_tracking: bool = True
    performance_log_interval: int = 100  # Log every N queries
    memory_usage_alert_threshold_mb: float = 500.0  # Alert if memory > 500MB
    
    def get_temporal_weight(self, timestamp: float) -> float:
        """Calculate temporal weight based on how recent the timestamp is."""
        current_time = time.time()
        age_hours = (current_time - timestamp) / 3600
        
        if age_hours <= self.current_time_window_hours:
            return self.recent_time_boost
        
        # Exponential decay for older data
        decay_periods = age_hours / self.current_time_window_hours
        return max(0.1, self.temporal_decay_factor ** decay_periods)
    
    def should_update_statistics(self, last_update_time: float, data_change_ratio: float) -> bool:
        """Determine if statistics should be updated."""
        time_since_update = time.time() - last_update_time
        
        # Force update if significant data change
        if data_change_ratio >= self.force_update_threshold:
            return True
        
        # Regular interval update
        return time_since_update >= self.batch_update_interval_seconds
    
    def validate_config(self) -> List[str]:
        """Validate configuration parameters and return any errors."""
        errors = []
        
        if not 0 < self.sampling_rate <= 1.0:
            errors.append("sampling_rate must be between 0 and 1")
        
        if self.min_sample_size <= 0:
            errors.append("min_sample_size must be positive")
        
        if self.max_sample_size < self.min_sample_size:
            errors.append("max_sample_size must be >= min_sample_size")
        
        if not 0 < self.temporal_decay_factor < 1.0:
            errors.append("temporal_decay_factor must be between 0 and 1")
        
        if self.recent_time_boost < 1.0:
            errors.append("recent_time_boost must be >= 1.0")
        
        return errors

# Default configuration instance
DEFAULT_CONFIG = OptimizationConfig()

# Predefined configurations for different scenarios
CONFIG_PRESETS = {
    "development": OptimizationConfig(
        sampling_rate=0.05,  # 5% for faster development
        batch_update_interval_seconds=60,  # 1 minute updates
        enable_performance_tracking=True
    ),
    
    "production": OptimizationConfig(
        sampling_rate=0.01,  # 1% for production efficiency
        batch_update_interval_seconds=300,  # 5 minute updates
        enable_performance_tracking=False
    ),
    
    "high_accuracy": OptimizationConfig(
        sampling_rate=0.02,  # 2% for better accuracy
        min_sample_size=2000,
        selectivity_threshold=0.05  # More aggressive optimization
    ),
    
    "memory_constrained": OptimizationConfig(
        sampling_rate=0.005,  # 0.5% to save memory
        max_sample_size=50000,
        max_adaptive_regions=50
    )
}

def get_config(preset_name: str = None) -> OptimizationConfig:
    """Get configuration by preset name or return default."""
    if preset_name and preset_name in CONFIG_PRESETS:
        return CONFIG_PRESETS[preset_name]
    return DEFAULT_CONFIG