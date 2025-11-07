#!/usr/bin/env python3

import tempfile
import subprocess
from datetime import datetime

# Simple test to debug Clojure script generation
def test_clojure_generation():
    # Create a simple operation
    operations_clj = [
        '[:xt/put {:xt/id :test-1 :name "Alice" :age 25} #inst "2023-01-01T10:00:00Z"]'
    ]
    
    # Build operations vector
    operations_str = "[\n  " + "\n  ".join(operations_clj) + "\n]"
    
    system_time = datetime.now()
    
    # Create Clojure script using correct XTDB v1 JDBC format with dialect
    submit_script = f'''
(require '[xtdb.api :as xt])

(def jdbc-node
  (xt/start-node
    {{:xtdb/tx-log {{:xtdb/module 'xtdb.jdbc/->tx-log
                    :connection-pool {{:dialect {{:xtdb/module 'xtdb.jdbc.psql/->dialect}}
                                      :db-spec {{:host "localhost"
                                                :port 5432
                                                :dbname "xtdb"
                                                :user "xtdbuser"
                                                :password "xtdbpass"}}}}}}
     :xtdb/document-store {{:xtdb/module 'xtdb.jdbc/->document-store
                           :connection-pool {{:dialect {{:xtdb/module 'xtdb.jdbc.psql/->dialect}}
                                             :db-spec {{:host "localhost"
                                                       :port 5432
                                                       :dbname "xtdb"
                                                       :user "xtdbuser"
                                                       :password "xtdbpass"}}}}}}}}))

(def txs
  [[{operations_str} {{:xt/tx-time #inst "{system_time.isoformat()}"}}]])

(doseq [[ops opts] txs]
  (xt/submit-tx jdbc-node ops opts))

(.close jdbc-node)

(println "Historical transaction submitted successfully")
'''
    
    # Write to file and check syntax
    with tempfile.NamedTemporaryFile(mode='w', suffix='.clj', delete=False) as f:
        f.write(submit_script)
        script_path = f.name
    
    print(f"Generated script saved to: {script_path}")
    print("\n=== Generated Clojure Script ===")
    print(submit_script)
    print("\n=== End Script ===")
    
    # Try to run it
    result = subprocess.run(
        ['clojure', '-M', script_path],
        capture_output=True,
        text=True,
        timeout=30
    )
    
    print(f"\nReturn code: {result.returncode}")
    print(f"STDOUT: {result.stdout}")
    print(f"STDERR: {result.stderr}")

if __name__ == "__main__":
    test_clojure_generation()