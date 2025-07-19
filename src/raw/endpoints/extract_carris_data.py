import os
import logging
import json
import zipfile
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
import requests
import pandas as pd

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CarrisAPIClient:
    BASE_URL_V2 = "https://api.carrismetropolitana.pt/v2"
    BASE_URL_V1 = "https://api.carrismetropolitana.pt"

    def __init__(self, timeout: int = 30, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Carris-Data-Pipeline/1.0',
            'Accept': 'application/json'
        })

    def get(self, endpoint: str) -> Union[Dict, List, bytes]:
        url_v2 = f"{self.BASE_URL_V2}{endpoint}"
        url_v1 = f"{self.BASE_URL_V1}{endpoint}"
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Requesting {url_v2} (attempt {attempt + 1}/{self.max_retries})")
                response = self.session.get(url_v2, timeout=self.timeout)
                response.raise_for_status()
                content_type = response.headers.get('Content-Type', '')
                if 'application/zip' in content_type or 'application/octet-stream' in content_type:
                    logger.info(f"Received file response from {endpoint}")
                    return response.content
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed on v2 (attempt {attempt + 1}): {str(e)}")
                if attempt == self.max_retries - 1:
                    logger.info(f"Trying fallback to v1: {url_v1}")
                    try:
                        response = self.session.get(url_v1, timeout=self.timeout)
                        response.raise_for_status()
                        content_type = response.headers.get('Content-Type', '')
                        if 'application/zip' in content_type or 'application/octet-stream' in content_type:
                            logger.info(f"Received file response from {endpoint} (v1 fallback)")
                            return response.content
                        return response.json()
                    except requests.exceptions.RequestException as e2:
                        logger.error(f"Failed to fetch {endpoint} from both v2 and v1: {str(e2)}")
                        raise
        return None

def discover_facility_endpoints(api_client: CarrisAPIClient) -> list:
    data = api_client.get("/facilities")
    facility_types = data.get("available_facilities", [])
    return [f"/facilities/{ft}" for ft in facility_types]

def extract_api_endpoint(api_client: CarrisAPIClient, endpoint: str) -> pd.DataFrame:
    logger.info(f"Extracting data from {endpoint}...")
    data = api_client.get(endpoint)
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict) and 'data' in data:
        df = pd.DataFrame(data['data'])
    else:
        df = pd.json_normalize(data)
    df['_extracted_at'] = datetime.now()
    df['_endpoint'] = endpoint
    for col in df.columns:
        if df[col].dtype == 'object':
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
                )
    logger.info(f"Extracted {len(df)} records from {endpoint}")
    return df

def validate_dataframe(df: pd.DataFrame, entity_name: str) -> Dict[str, Any]:
    validation = {
        "entity": entity_name,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": df.columns.tolist(),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
        "null_percentage": (df.isnull().sum() / len(df) * 100).to_dict() if len(df) > 0 else {}
    }
    empty_columns = df.columns[df.isnull().all()].tolist()
    if empty_columns:
        validation["empty_columns"] = empty_columns
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        validation["duplicate_rows"] = duplicate_count
    return validation

def extract_and_save_endpoint(api_client, endpoint, output_dir, partition_value=None):
    if partition_value is None:
        partition_value = datetime.now().strftime("%Y%m%d")
    partition_dir = os.path.join(output_dir, f"extracted_at={partition_value}")
    os.makedirs(partition_dir, exist_ok=True)
    entity_name = endpoint.replace("/", "_").strip("_")
    try:
        df = extract_api_endpoint(api_client, endpoint)
        validation = validate_dataframe(df, endpoint.replace("/", ""))
        output_path = os.path.join(partition_dir, f"{entity_name}.parquet")
        df.to_parquet(output_path, index=False, engine="pyarrow")
        logger.info(f"Saved {entity_name} to {output_path}")
        return {"status": "success", "records": len(df), "output_path": output_path, "validation": validation}
    except Exception as e:
        logger.error(f"Failed to extract {endpoint}: {str(e)}")
        return {"status": "failed", "error": str(e)}

																		
							   
														   
											 
																				  
										
				
											
																	 
																 
																								   
																
				  

def extract_gtfs_data(api_client: CarrisAPIClient) -> Dict[str, pd.DataFrame]:
    logger.info("Extracting GTFS data...")
    zip_content = api_client.get("/gtfs")
    if not isinstance(zip_content, bytes):
        raise ValueError("Expected zip file content from /gtfs endpoint")
    gtfs_data = {}
    tmp_file = tempfile.NamedTemporaryFile(suffix='.zip', delete=False)
    tmp_file.write(zip_content)
    tmp_file.flush()
    temp_zip_path = tmp_file.name
    tmp_file.close()
    try:
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_file:
            logger.info(f"GTFS zip contains: {zip_file.namelist()}")
            for filename in zip_file.namelist():
                if filename.endswith('.txt'):
                    table_name = filename.replace('.txt', '')
                    try:
                        with zip_file.open(filename) as file:
                            df = pd.read_csv(file, low_memory=False)
                            df['_extracted_at'] = datetime.now()
                            df['_source_file'] = filename
                            gtfs_data[table_name] = df
                            logger.info(f"Extracted {table_name}: {len(df)} records")
                    except Exception as e:
                        logger.error(f"Failed to process {filename}: {str(e)}")
    finally:
        try:
            os.unlink(temp_zip_path)
        except Exception as cleanup_error:
            logger.warning(f"Could not delete temp file: {str(cleanup_error)}")
    return gtfs_data

def extract_and_save_gtfs(api_client, output_dir, partition_value=None):
    if partition_value is None:
        partition_value = datetime.now().strftime("%Y%m%d")
    gtfs_data = extract_gtfs_data(api_client)
    gtfs_dir = os.path.join(output_dir, "gtfs", f"extracted_at={partition_value}")
    os.makedirs(gtfs_dir, exist_ok=True)
    results = {}
    for table_name, df in gtfs_data.items():
        output_path = os.path.join(gtfs_dir, f"{table_name}.parquet")
        df.to_parquet(output_path, index=False, engine="pyarrow")
        results[table_name] = {"status": "success", "records": len(df), "output_path": output_path}
        logger.info(f"Saved GTFS {table_name} to {output_path}")
    return results

def extract_selected_data(
    endpoints=None,
    output_dir: str = "./raw_data"
):
    api_client = CarrisAPIClient()
    partition_value = datetime.now().strftime("%Y%m%d")
    results = {}

    # Se endpoints não especificados, faz batch completo + GTFS
    if endpoints is None:
        facility_endpoints = discover_facility_endpoints(api_client)
        endpoints = [
            "/regions", "/districts", "/municipalities", "/stops", "/lines",
            "/routes", "/patterns", "/shapes", "/trips", "/vehicles", "/facilities",
            *facility_endpoints,
            "/schools", "/service_alerts", "/realtime_arrivals"
        ]
        batch_mode = True
    else:
        # Normaliza endpoints, permite gtfs explícito
        batch_mode = False
        endpoints = [ep.lower() for ep in endpoints]
        gtfs_included = ("gtfs" in [ep.replace("/", "") for ep in endpoints])

    # Extrai todos os endpoints normais (exceto gtfs)
    for endpoint in endpoints:
        if endpoint.lower().replace("/", "") == "gtfs":
            continue  # Não tentar extrair gtfs como endpoint "normal"
        result = extract_and_save_endpoint(api_client, endpoint, output_dir, partition_value)
        results[endpoint] = result

    # Extrai GTFS se batch, ou se gtfs está nos endpoints pedidos explicitamente
    if (batch_mode or ("gtfs" in [ep.replace("/", "") for ep in endpoints])):
        logger.info("Extracting GTFS ZIP ...")
        gtfs_results = extract_and_save_gtfs(api_client, output_dir, partition_value)
        results["/gtfs"] = gtfs_results

    return results

# --- MAIN ---
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Carris Data Extractor")
    parser.add_argument(
        "--endpoint", type=str, default=None, nargs="*",
        help="API endpoint(s) to extract (e.g. /stops or gtfs). If omitted, extracts all and GTFS."
    )
    args = parser.parse_args()

    if args.endpoint:
									  
        endpoints = args.endpoint
        results = extract_selected_data(endpoints=endpoints)
        print("\n=== Extraction Summary (Selected Endpoints) ===")
        for endpoint, info in results.items():
            if isinstance(info, dict):
                if "status" in info:
                    status = "✓" if info["status"] == "success" else "✗"
                    records = info.get("records", 0)
                    print(f"{status} {endpoint}: {records} records")
                else:
                    # Caso especial GTFS
                    for table, tabinfo in info.items():
                        status = "✓" if tabinfo["status"] == "success" else "✗"
                        records = tabinfo.get("records", 0)
                        print(f"{status} GTFS {table}: {records} records")
            else:
                print(f"{endpoint}: {info}")
    else:
        results = extract_selected_data()
        print("\n=== Extraction Summary (All Endpoints + GTFS) ===")
        for endpoint, info in results.items():
            if endpoint == "/gtfs":
                print("\n## GTFS Tables:")
                for table, tabinfo in info.items():
                    status = "✓" if tabinfo["status"] == "success" else "✗"
                    records = tabinfo.get("records", 0)
                    print(f"{status} {table}: {records} records")
            else:
                status = "✓" if info["status"] == "success" else "✗"
                records = info.get("records", 0)
                print(f"{status} {endpoint}: {records} records")
        print("\nTIP: Use --endpoint /endpoint or gtfs to re-extract only one endpoint or GTFS.")
