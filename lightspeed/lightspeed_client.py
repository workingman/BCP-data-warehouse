import requests
import time
import logging
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin
from ratelimit import limits, sleep_and_retry

logger = logging.getLogger(__name__)


class LightspeedClient:
    """Client for interacting with Lightspeed X-Series API"""
    
    API_VERSION = "2.0"
    CALLS_PER_SECOND = 5  # API rate limit
    
    def __init__(self, domain: str, access_token: str):
        """
        Initialize Lightspeed API client
        
        Args:
            domain: Your Lightspeed domain (e.g., 'store.vendhq.com')
            access_token: Personal access token for authentication
        """
        self.domain = domain
        self.base_url = f"https://{domain}/api/{self.API_VERSION}/"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    @sleep_and_retry
    @limits(calls=CALLS_PER_SECOND, period=1)
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make rate-limited API request
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response data
        """
        url = urljoin(self.base_url, endpoint)
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {endpoint}: {str(e)}")
            raise
    
    def get_paginated_data(self, endpoint: str, params: Optional[Dict] = None, 
                          start_page: int = 1, checkpoint_callback=None) -> List[Dict]:
        """
        Fetch all pages of data from a paginated endpoint (legacy method - buffers all data)
        
        Args:
            endpoint: API endpoint path
            params: Initial query parameters
            start_page: Page to start from (for resuming)
            checkpoint_callback: Function to call after each page for checkpointing
            
        Returns:
            List of all records from all pages
        """
        all_data = []
        for page_data in self.stream_paginated_data(endpoint, params, start_page, checkpoint_callback):
            all_data.extend(page_data)
        return all_data
    
    def stream_paginated_data(self, endpoint: str, params: Optional[Dict] = None, 
                             start_page: int = 1, checkpoint_callback=None, 
                             data_callback=None):
        """
        Stream data from an endpoint using monolithic requests to bypass broken pagination
        
        Args:
            endpoint: API endpoint path
            params: Initial query parameters
            start_page: Page to start from (ignored - using monolithic approach)
            checkpoint_callback: Function to call after data fetch for checkpointing  
            data_callback: Function to call with data for streaming processing
            
        Yields:
            List of all records from the endpoint in a single batch
        """
        if params is None:
            params = {}
        
        # Use monolithic approach: request all data in single call with large page_size
        # This bypasses Lightspeed's broken pagination entirely
        params['page_size'] = 50000  # Large enough for most datasets
        if 'page' in params:
            del params['page']  # Remove pagination entirely
        
        try:
            logger.info(f"Fetching all {endpoint} data (monolithic request)")
            response = self._make_request(endpoint, params)
            
            # Handle different response structures
            if 'data' in response:
                data = response['data']
            elif endpoint in response:
                data = response[endpoint]
            else:
                # Some endpoints return the array directly
                data = response if isinstance(response, list) else []
            
            if not data:
                logger.info(f"No data returned from {endpoint}")
                return
            
            total_records = len(data)
            logger.info(f"Retrieved {total_records} records from {endpoint} in single request")
            
            # Check if we hit the page_size limit exactly (indicates potential missing data)
            if total_records == params['page_size'] and params['page_size'] < 50000:
                logger.warning(f"⚠️  {endpoint}: Got exactly {total_records} records (page_size limit). "
                             f"This may indicate missing data beyond the API limit. "
                             f"Expected more records but API may have a hard limit.")
            
            # Call data callback for streaming processing
            if data_callback:
                data_callback(data, 1, total_records)
            
            # Yield all data as a single batch
            yield data
            
            # Call checkpoint callback if provided
            if checkpoint_callback:
                checkpoint_callback(endpoint, 1, total_records)
                
        except KeyboardInterrupt:
            logger.info(f"Export interrupted at {endpoint}")
            if checkpoint_callback:
                checkpoint_callback(endpoint, 1, total_records)  # Save progress before interruption
            raise
        except Exception as e:
            logger.error(f"Failed to fetch {endpoint}: {str(e)}")
            raise
    
    # Customer endpoints
    def get_customers(self) -> List[Dict]:
        return self.get_paginated_data('customers')
    
    def get_customer_groups(self) -> List[Dict]:
        return self.get_paginated_data('customer_groups')
    
    # Product endpoints
    def get_products(self) -> List[Dict]:
        return self.get_paginated_data('products')
    
    def get_product_types(self) -> List[Dict]:
        return self.get_paginated_data('product_types')
    
    def get_brands(self) -> List[Dict]:
        return self.get_paginated_data('brands')
    
    def get_suppliers(self) -> List[Dict]:
        return self.get_paginated_data('suppliers')
    
    # Sales endpoints
    def get_sales(self, since: Optional[str] = None) -> List[Dict]:
        params = {}
        if since:
            params['since'] = since
        return self.get_paginated_data('sales', params)
    
    def get_sale_payments(self) -> List[Dict]:
        return self.get_paginated_data('payments')
    
    # Inventory endpoints
    def get_inventory(self) -> List[Dict]:
        return self.get_paginated_data('inventory')
    
    def get_consignments(self) -> List[Dict]:
        return self.get_paginated_data('consignments')
    
    # Outlet and register endpoints
    def get_outlets(self) -> List[Dict]:
        return self.get_paginated_data('outlets')
    
    def get_registers(self) -> List[Dict]:
        return self.get_paginated_data('registers')
    
    def get_register_closures(self) -> List[Dict]:
        return self.get_paginated_data('register_sales')
    
    # Financial endpoints
    def get_taxes(self) -> List[Dict]:
        return self.get_paginated_data('taxes')
    
    def get_payment_types(self) -> List[Dict]:
        return self.get_paginated_data('payment_types')
    
    def get_price_books(self) -> List[Dict]:
        return self.get_paginated_data('price_books')
    
    def get_promotions(self) -> List[Dict]:
        return self.get_paginated_data('promotions')
    
    # User endpoints
    def get_users(self) -> List[Dict]:
        return self.get_paginated_data('users')
    
    # Gift card endpoints
    def get_gift_cards(self) -> List[Dict]:
        return self.get_paginated_data('gift_cards')