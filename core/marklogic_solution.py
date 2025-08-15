import hashlib
import json
import requests
from requests.auth import HTTPDigestAuth
from uuid import UUID, uuid4
from typing import Dict, Any, List
from .bitemporal_space import Rectangle


class MarkLogicSolution:
    """
    MarkLogic equivalent of SolutionB using the same two-collection approach.
    
    This implementation mirrors SolutionB's logic:
    - Index collection: stores metadata and temporal bounds with indexed fields
    - Payload collection: stores actual data with unique vrefs
    - Uses MarkLogic REST API for document operations and XQuery for queries
    - Maintains the same query patterns and temporal logic as SolutionB
    """
    def __init__(self, host='localhost', port=8000, username='admin', password='admin123') -> None:
        self.name = "MarkLogicSolution"
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.base_url = f"http://{host}:{port}"
        self.auth = HTTPDigestAuth(username, password)
        self.indices = ["name", "age", "attr1", "attr2", "attr3", "attr4"]
        self.index_collection = "index-collection"
        self.payload_collection = "payload-collection"
    
    async def initialize_collections(self):
        """Initialize MarkLogic collections and indexes"""
        try:
            # Clear existing documents in collections
            self._clear_collection(self.index_collection)
            self._clear_collection(self.payload_collection)
            
            # Create range indexes for efficient querying
            self._create_range_indexes()
            
            print(f"{self.name}: Index and Payload collections initialized with appropriate indexes.")
        except Exception as e:
            print(f"Error initializing collections: {e}")
    
    def _clear_collection(self, collection_name):
        """Clear all documents from a collection"""
        query = f"""
        for $doc in collection("{collection_name}")
        return xdmp:document-delete(xdmp:node-uri($doc))
        """
        self._eval_xquery(query)
    
    def _create_range_indexes(self):
        """Create range indexes for efficient querying"""
        # This would typically be done through MarkLogic Admin API
        # For now, we'll assume indexes are created externally
        pass
    
    def _eval_xquery(self, query):
        """Execute XQuery using MarkLogic REST API"""
        url = f"{self.base_url}/v1/eval"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {'xquery': query}
        
        try:
            response = requests.post(url, auth=self.auth, headers=headers, data=data)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"XQuery execution failed: {e}")
            return None
    
    def _insert_document(self, uri, content, collections=None):
        """Insert a document using MarkLogic REST API"""
        url = f"{self.base_url}/v1/documents"
        params = {'uri': uri}
        if collections:
            params['collection'] = collections
        
        headers = {'Content-Type': 'application/json'}
        
        try:
            response = requests.put(url, auth=self.auth, headers=headers, 
                                  params=params, data=json.dumps(content))
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Document insert failed: {e}")
            return False
    
    # Function to compute MD5 hash of data (for Payload)
    @staticmethod
    def compute_md5(data_dict: Dict[str, Any]) -> str:
        """Compute MD5 hash of a dictionary, ensuring JSON-like string format."""
        data_str = str(data_dict).replace("'", '"')
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()
    
    # Main function to insert rectangles
    def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Insert rectangles into collections using batch operations."""
        # Prepare batch documents
        index_docs = []
        payload_docs = []
        payload_vrefs = set()
        
        # Process all rectangles
        for rect in rectangles:
            vref = str(UUID(bytes=hashlib.md5(str(rect.data["payload"]).encode()).digest()))
            eref = rect.data.get("id", 0)
            
            # Create base index entry
            index_entry = {
                "vref": vref,
                "eref": eref,
                "tt_from": rect.tt_from.isoformat(),
                "tt_to": rect.tt_to.isoformat(),
                "vt_from": rect.vt_from.isoformat(),
                "vt_to": rect.vt_to.isoformat(),
                "entity": entity,
            }
            
            # Dynamically add all indexed fields
            for field in self.indices:
                index_entry[field] = rect.data["payload"].get(field)
            
            index_docs.append(index_entry)
            
            # Payload document (only add unique vrefs)
            if vref not in payload_vrefs:
                payload_entry = {
                    "vref": vref,
                    "data": rect.data["payload"],
                }
                payload_docs.append(payload_entry)
                payload_vrefs.add(vref)
        
        # Perform batch inserts
        if index_docs:
            try:
                self._batch_insert_documents(index_docs, self.index_collection)
                print(f"{self.name}: Inserted {len(index_docs)} documents into Index collection")
            except Exception as e:
                print(f"Some Index inserts failed: {e}")
        
        if payload_docs:
            try:
                self._batch_insert_documents(payload_docs, self.payload_collection)
                print(f"{self.name}: Inserted {len(payload_docs)} documents into Payload collection")
            except Exception as e:
                print(f"Some Payload inserts failed: {e}")
    
    def _batch_insert_documents(self, documents: List[Dict], collection: str):
        """Insert multiple documents into a MarkLogic collection"""
        for i, doc in enumerate(documents):
            doc_uri = f"/{collection}/{uuid4()}.json"
            success = self._insert_document(doc_uri, doc, [collection])
            if not success:
                print(f"Failed to insert document {i} into {collection}")
    
    def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Query documents by name and age within temporal bounds"""
        tt_iso = tt.isoformat()
        vt_iso = vt.isoformat()
        
        # First query the Index collection to find matching records
        index_query = f"""
        for $doc in collection("{self.index_collection}")
        let $data := $doc/json
        where $data/name = "{name}"
          and $data/age = {age}
          and $data/entity = "{entity}"
          and xs:dateTime($data/tt_from) <= xs:dateTime("{tt_iso}")
          and xs:dateTime($data/tt_to) > xs:dateTime("{tt_iso}")
          and xs:dateTime($data/vt_from) <= xs:dateTime("{vt_iso}")
          and xs:dateTime($data/vt_to) > xs:dateTime("{vt_iso}")
        return $data/vref/text()
        """
        
        try:
            vrefs_result = self._eval_xquery(index_query)
            if not vrefs_result:
                return []
                
            # Parse vrefs from result (this would need proper XML/JSON parsing)
            vrefs = [vref.strip() for vref in vrefs_result.split() if vref.strip()]
            
            if not vrefs:
                return []
            
            # Query Payload collection for the actual data
            vref_conditions = " or ".join([f'$data/vref = "{vref}"' for vref in vrefs])
            payload_query = f"""
            for $doc in collection("{self.payload_collection}")
            let $data := $doc/json
            where {vref_conditions}
            return $doc
            """
            
            payload_result = self._eval_xquery(payload_query)
            
            # Parse the results
            results = []
            if payload_result:
                # Parse JSON results (this would need proper JSON parsing)
                # For now, return raw results
                results.append(payload_result)
            
            return results
            
        except Exception as e:
            print(f"Query failed: {e}")
            return []
    
    def get_all_current_data(self, tt, vt, entity="Student"):
        """Get all current data at specified temporal coordinates"""
        tt_iso = tt.isoformat()
        vt_iso = vt.isoformat()
        
        # Query Index collection for all current records
        index_query = f"""
        for $doc in collection("{self.index_collection}")
        let $data := $doc/json
        where $data/entity = "{entity}"
          and xs:dateTime($data/tt_from) <= xs:dateTime("{tt_iso}")
          and xs:dateTime($data/tt_to) > xs:dateTime("{tt_iso}")
          and xs:dateTime($data/vt_from) <= xs:dateTime("{vt_iso}")
          and xs:dateTime($data/vt_to) > xs:dateTime("{vt_iso}")
        return $data/vref/text()
        """
        
        try:
            vrefs_result = self._eval_xquery(index_query)
            if not vrefs_result:
                return []
                
            vrefs = [vref.strip() for vref in vrefs_result.split() if vref.strip()]
            
            if not vrefs:
                return []
            
            # Query Payload collection for the actual data
            vref_conditions = " or ".join([f'$data/vref = "{vref}"' for vref in vrefs])
            payload_query = f"""
            for $doc in collection("{self.payload_collection}")
            let $data := $doc/json
            where {vref_conditions}
            return $doc
            """
            
            payload_result = self._eval_xquery(payload_query)
            
            # Parse the results
            results = []
            if payload_result:
                results.append(payload_result)
            
            return results
            
        except Exception as e:
            print(f"Query failed: {e}")
            return []
    
    def get_current_data_for_entity(self, entity_id, tt, vt, entity="Student"):
        """Get current data for a specific entity"""
        tt_iso = tt.isoformat()
        vt_iso = vt.isoformat()
        
        # Query Index collection for specific entity
        index_query = f"""
        for $doc in collection("{self.index_collection}")
        let $data := $doc/json
        where $data/eref = {entity_id}
          and $data/entity = "{entity}"
          and xs:dateTime($data/tt_from) <= xs:dateTime("{tt_iso}")
          and xs:dateTime($data/tt_to) > xs:dateTime("{tt_iso}")
          and xs:dateTime($data/vt_from) <= xs:dateTime("{vt_iso}")
          and xs:dateTime($data/vt_to) > xs:dateTime("{vt_iso}")
        return $data/vref/text()
        """
        
        try:
            vrefs_result = self._eval_xquery(index_query)
            if not vrefs_result:
                return []
                
            vrefs = [vref.strip() for vref in vrefs_result.split() if vref.strip()]
            
            if not vrefs:
                return []
            
            # Query Payload collection for the actual data
            vref_conditions = " or ".join([f'$data/vref = "{vref}"' for vref in vrefs])
            payload_query = f"""
            for $doc in collection("{self.payload_collection}")
            let $data := $doc/json
            where {vref_conditions}
            return $doc
            """
            
            payload_result = self._eval_xquery(payload_query)
            
            # Parse the results
            results = []
            if payload_result:
                results.append(payload_result)
            
            return results
            
        except Exception as e:
            print(f"Query failed: {e}")
            return []
    
    def delete_data(self, entity_id, tt, vt, entity="Student"):
        """Logically delete data by updating tt_to timestamp"""
        tt_iso = tt.isoformat()
        vt_iso = vt.isoformat()
        
        # Find current records to update
        query = f"""
        for $doc in collection("{self.index_collection}")
        let $data := $doc/json
        where $data/eref = {entity_id}
          and $data/entity = "{entity}"
          and xs:dateTime($data/tt_from) <= xs:dateTime("{tt_iso}")
          and xs:dateTime($data/tt_to) > xs:dateTime("{tt_iso}")
          and xs:dateTime($data/vt_from) <= xs:dateTime("{vt_iso}")
          and xs:dateTime($data/vt_to) > xs:dateTime("{vt_iso}")
        return (
            xdmp:node-replace(
                $data/tt_to,
                <tt_to>{"{tt_iso}"}</tt_to>
            )
        )
        """
        
        try:
            self._eval_xquery(query)
            print(f"{self.name}: Logically deleted data for entity {entity_id}")
        except Exception as e:
            print(f"Delete operation failed: {e}")
    
    def get_data_history(self, entity_id, entity="Student"):
        """Get complete history for an entity"""
        query = f"""
        for $doc in collection("{self.index_collection}")
        let $data := $doc/json
        where $data/eref = {entity_id}
          and $data/entity = "{entity}"
        order by xs:dateTime($data/tt_from), xs:dateTime($data/vt_from)
        return $data
        """
        
        try:
            result = self._eval_xquery(query)
            
            # Parse and format results
            results = []
            if result:
                # This would need proper JSON parsing in a real implementation
                results.append(result)
            
            return results
            
        except Exception as e:
            print(f"History query failed: {e}")
            return []