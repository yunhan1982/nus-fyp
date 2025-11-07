#!/usr/bin/env python3
"""
Python wrapper to run the XTDB transaction log generation and replay prototype.
This script executes the Clojure prototype and demonstrates custom system time control.
"""

import subprocess
import os
import sys
from datetime import datetime
import json

def run_clojure_prototype():
    """Run the Clojure prototype for transaction log generation and replay"""
    
    print("=== XTDB Transaction Log Generation and Replay Prototype ===")
    print("This prototype demonstrates:")
    print("1. Generating XTDB transaction logs with custom system times")
    print("2. Replaying transaction logs on new XTDB nodes")
    print("3. Querying data at different bitemporal points")
    print("\n" + "="*60 + "\n")
    
    # Check if Clojure is available
    try:
        result = subprocess.run(['clojure', '--version'], 
                              capture_output=True, text=True, check=True)
        print(f"Clojure version: {result.stdout.strip()}")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("ERROR: Clojure CLI not found. Please install Clojure CLI tools.")
        print("Visit: https://clojure.org/guides/getting_started")
        return False
    
    # Check if deps.edn exists
    if not os.path.exists('deps.edn'):
        print("ERROR: deps.edn not found. Please ensure you're in the project root.")
        return False
    
    try:
        # Run the Clojure prototype
        print("\nStarting Clojure prototype...")
        
        clojure_code = """
        (load-file "generate_tx_log_prototype.clj")
        (in-ns 'generate-tx-log-prototype)
        (run-prototype)
        """
        
        # Execute the Clojure code
        process = subprocess.Popen(
            ['clojure', '-M', '-e', clojure_code],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Print output in real-time
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip())
        
        # Get any remaining output
        stdout, stderr = process.communicate()
        if stdout:
            print(stdout)
        if stderr:
            print(f"STDERR: {stderr}")
        
        return_code = process.returncode
        
        if return_code == 0:
            print("\n✅ Prototype completed successfully!")
            return True
        else:
            print(f"\n❌ Prototype failed with return code: {return_code}")
            return False
            
    except Exception as e:
        print(f"\n❌ Error running prototype: {e}")
        return False

def inspect_generated_files():
    """Inspect the generated transaction log files"""
    
    print("\n=== Inspecting Generated Files ===")
    
    tx_log_dir = "/tmp/xtdb-custom-tx-log"
    doc_store_dir = "/tmp/xtdb-custom-doc-store"
    replay_doc_store_dir = "/tmp/xtdb-replay-doc-store"
    
    for dir_path, description in [
        (tx_log_dir, "Transaction Log"),
        (doc_store_dir, "Original Document Store"),
        (replay_doc_store_dir, "Replay Document Store")
    ]:
        print(f"\n{description} ({dir_path}):")
        
        if os.path.exists(dir_path):
            try:
                # Get directory size
                total_size = 0
                file_count = 0
                
                for dirpath, dirnames, filenames in os.walk(dir_path):
                    for filename in filenames:
                        filepath = os.path.join(dirpath, filename)
                        total_size += os.path.getsize(filepath)
                        file_count += 1
                
                print(f"  ✅ Directory exists")
                print(f"  📁 Files: {file_count}")
                print(f"  💾 Size: {total_size / 1024:.2f} KB")
                
                # List some files
                if file_count > 0:
                    print(f"  📄 Sample files:")
                    count = 0
                    for dirpath, dirnames, filenames in os.walk(dir_path):
                        for filename in filenames[:3]:  # Show first 3 files
                            if count < 3:
                                filepath = os.path.join(dirpath, filename)
                                size = os.path.getsize(filepath)
                                print(f"    - {filename} ({size} bytes)")
                                count += 1
                
            except Exception as e:
                print(f"  ❌ Error inspecting directory: {e}")
        else:
            print(f"  ❌ Directory does not exist")

def demonstrate_advanced_usage():
    """Demonstrate advanced usage patterns"""
    
    print("\n=== Advanced Usage Demonstration ===")
    
    advanced_clojure_code = """
    (load-file "generate_tx_log_prototype.clj")
    (in-ns 'generate-tx-log-prototype)
    
    ;; Demonstrate inspection of transaction log
    (println "\n=== Inspecting Transaction Log ===")
    (try
      (inspect-tx-log "/tmp/xtdb-custom-tx-log")
      (catch Exception e
        (println (str "Error inspecting log: " (.getMessage e)))))
    
    ;; Demonstrate advanced transaction generation
    (println "\n=== Advanced Transaction Generation ===")
    (let [advanced-txs
          [{:tx-ops [[:xtdb.api/put {:xt/id :advanced-1
                                     :type "advanced"
                                     :timestamp "2024-01-01T10:00:00Z"}]]
            :valid-time #inst "2024-01-01T10:00:00.000-00:00"
            :system-time #inst "2024-01-01T10:00:00.000-00:00"
            :tx-id 1000}
           
           {:tx-ops [[:xtdb.api/put {:xt/id :advanced-2
                                     :type "advanced"
                                     :timestamp "2024-01-02T10:00:00Z"}]]
            :valid-time #inst "2024-01-02T10:00:00.000-00:00"
            :system-time #inst "2024-01-02T10:00:00.000-00:00"
            :tx-id 1001}]]
      
      (try
        (generate-advanced-tx-log "/tmp/xtdb-advanced-tx-log" 
                                  "/tmp/xtdb-advanced-doc-store" 
                                  advanced-txs)
        (println "Advanced transaction log generated successfully!")
        (catch Exception e
          (println (str "Error generating advanced log: " (.getMessage e))))))
    """
    
    try:
        print("\nRunning advanced demonstration...")
        
        process = subprocess.run(
            ['clojure', '-M', '-e', advanced_clojure_code],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if process.stdout:
            print(process.stdout)
        if process.stderr:
            print(f"STDERR: {process.stderr}")
            
        if process.returncode == 0:
            print("✅ Advanced demonstration completed successfully!")
        else:
            print(f"❌ Advanced demonstration failed with return code: {process.returncode}")
            
    except subprocess.TimeoutExpired:
        print("❌ Advanced demonstration timed out")
    except Exception as e:
        print(f"❌ Error running advanced demonstration: {e}")

def cleanup_temp_files():
    """Clean up temporary files created during the demonstration"""
    
    print("\n=== Cleanup ===")
    
    temp_dirs = [
        "/tmp/xtdb-custom-tx-log",
        "/tmp/xtdb-custom-doc-store", 
        "/tmp/xtdb-replay-doc-store",
        "/tmp/xtdb-advanced-tx-log",
        "/tmp/xtdb-advanced-doc-store",
        "/tmp/inspect-doc-store"
    ]
    
    for temp_dir in temp_dirs:
        if os.path.exists(temp_dir):
            try:
                import shutil
                shutil.rmtree(temp_dir)
                print(f"✅ Cleaned up: {temp_dir}")
            except Exception as e:
                print(f"❌ Failed to clean up {temp_dir}: {e}")
        else:
            print(f"ℹ️  Directory not found: {temp_dir}")

def main():
    """Main function to run the complete demonstration"""
    
    print(f"Starting XTDB Transaction Log Prototype at {datetime.now()}")
    print(f"Working directory: {os.getcwd()}")
    
    # Step 1: Run the main prototype
    success = run_clojure_prototype()
    
    if success:
        # Step 2: Inspect generated files
        inspect_generated_files()
        
        # Step 3: Demonstrate advanced usage
        demonstrate_advanced_usage()
    
    # Step 4: Cleanup (optional)
    response = input("\nDo you want to clean up temporary files? (y/N): ")
    if response.lower() in ['y', 'yes']:
        cleanup_temp_files()
    else:
        print("Temporary files preserved for inspection.")
        print("You can manually inspect:")
        print("  - /tmp/xtdb-custom-tx-log (Transaction Log)")
        print("  - /tmp/xtdb-custom-doc-store (Original Document Store)")
        print("  - /tmp/xtdb-replay-doc-store (Replay Document Store)")
    
    print("\n🎉 Demonstration completed!")

if __name__ == "__main__":
    main()