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
    
    def get_paginated_data(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Fetch all pages of data from a paginated endpoint
        
        Args:
            endpoint: API endpoint path
            params: Initial query parameters
            
        Returns:
            List of all records from all pages
        """
        if params is None:
            params = {}
        
        all_data = []
        page = 1
        
        while True:
            params['page'] = page
            params['page_size'] = 200  # Max page size for most endpoints
            
            logger.info(f"Fetching {endpoint} page {page}")
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
                break
                
            all_data.extend(data)
            
            # Check for more pages
            if 'pagination' in response:
                if response['pagination']['page'] >= response['pagination']['pages']:
                    break
            elif len(data) < params['page_size']:
                break
                
            page += 1
            
        logger.info(f"Fetched {len(all_data)} records from {endpoint}")
        return all_data
    
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