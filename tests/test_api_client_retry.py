"""
Unit Tests for TradeStation API Client 401 Retry Logic

Tests that the client automatically retries requests when access tokens expire (401 errors).
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tradestation.api.client import TradeStationClient
from tradestation.api.config import TradeStationConfig
from tradestation.api.auth import TradeStationAuth


class TestAPIClientRetryLogic(unittest.TestCase):
    """Test automatic 401 retry with token refresh."""
    
    def setUp(self):
        """Set up mock config and client."""
        # Create mock config
        self.mock_config = Mock(spec=TradeStationConfig)
        self.mock_config.base_url = "https://sim-api.tradestation.com"
        
        # Create client with mock config
        self.client = TradeStationClient(self.mock_config)
        
        # Mock the auth module to avoid real token refresh
        self.client.auth = Mock(spec=TradeStationAuth)
        self.client.auth.get_access_token = Mock(return_value="valid_token")
        self.client.auth.clear_token_cache = Mock()
    
    @patch('tradestation.api.client.requests')
    def test_get_request_retries_on_401(self, mock_requests):
        """Test that GET requests retry once on 401 error."""
        # First call: 401 error
        first_response = Mock()
        first_response.ok = False
        first_response.status_code = 401
        first_response.json = Mock(return_value={'Message': 'Access token has expired'})
        first_response.text = 'Access token has expired'
        
        # Second call (after retry): 200 success
        second_response = Mock()
        second_response.ok = True
        second_response.json = Mock(return_value={'data': 'success'})
        
        # Mock requests.get to return 401 first, then 200
        mock_requests.get = Mock(side_effect=[first_response, second_response])
        
        # Make the request
        result = self.client.get('/v3/test')
        
        # Verify retry happened
        self.assertEqual(mock_requests.get.call_count, 2, "Should retry once on 401")
        self.client.auth.clear_token_cache.assert_called_once()
        self.assertEqual(result, {'data': 'success'})
    
    @patch('tradestation.api.client.requests')
    def test_post_request_retries_on_401(self, mock_requests):
        """Test that POST requests retry once on 401 error."""
        # First call: 401 error
        first_response = Mock()
        first_response.ok = False
        first_response.status_code = 401
        first_response.json = Mock(return_value={'Message': 'Unauthorized'})
        first_response.text = 'Unauthorized'
        
        # Second call: 200 success
        second_response = Mock()
        second_response.ok = True
        second_response.json = Mock(return_value={'OrderID': '12345'})
        
        mock_requests.post = Mock(side_effect=[first_response, second_response])
        
        # Make the request
        result = self.client.post('/v3/test', json_data={'test': 'data'})
        
        # Verify retry happened
        self.assertEqual(mock_requests.post.call_count, 2, "Should retry once on 401")
        self.client.auth.clear_token_cache.assert_called_once()
        self.assertEqual(result, {'OrderID': '12345'})
    
    @patch('tradestation.api.client.requests')
    def test_delete_request_retries_on_401(self, mock_requests):
        """Test that DELETE requests retry once on 401 error."""
        # First call: 401 error
        first_response = Mock()
        first_response.ok = False
        first_response.status_code = 401
        first_response.json = Mock(return_value={'error': 'token expired'})
        first_response.text = 'token expired'
        
        # Second call: 200 success
        second_response = Mock()
        second_response.ok = True
        second_response.json = Mock(return_value={'status': 'deleted'})
        
        mock_requests.delete = Mock(side_effect=[first_response, second_response])
        
        # Make the request
        result = self.client.delete('/v3/test/123')
        
        # Verify retry happened
        self.assertEqual(mock_requests.delete.call_count, 2, "Should retry once on 401")
        self.client.auth.clear_token_cache.assert_called_once()
        self.assertEqual(result, {'status': 'deleted'})
    
    @patch('tradestation.api.client.requests')
    def test_non_401_error_not_retried(self, mock_requests):
        """Test that non-401 errors are not retried."""
        # 400 Bad Request error
        error_response = Mock()
        error_response.ok = False
        error_response.status_code = 400
        error_response.json = Mock(return_value={'Message': 'Bad request'})
        error_response.text = 'Bad request'
        
        mock_requests.get = Mock(return_value=error_response)
        
        # Should raise exception without retry
        with self.assertRaises(Exception) as context:
            self.client.get('/v3/test')
        
        # Verify no retry (only 1 call)
        self.assertEqual(mock_requests.get.call_count, 1, "Should not retry non-401 errors")
        self.client.auth.clear_token_cache.assert_not_called()
        self.assertIn('400', str(context.exception))
    
    @patch('tradestation.api.client.requests')
    def test_persistent_401_not_infinite_retry(self, mock_requests):
        """Test that persistent 401 errors don't cause infinite retry."""
        # Always return 401 (bad credentials scenario)
        error_response = Mock()
        error_response.ok = False
        error_response.status_code = 401
        error_response.json = Mock(return_value={'Message': 'Invalid credentials'})
        error_response.text = 'Invalid credentials'
        
        mock_requests.get = Mock(return_value=error_response)
        
        # Should raise exception after 1 retry (2 total attempts)
        with self.assertRaises(Exception) as context:
            self.client.get('/v3/test')
        
        # Verify only retried once (2 total calls)
        self.assertEqual(mock_requests.get.call_count, 2, "Should retry once then give up")
        self.client.auth.clear_token_cache.assert_called_once()
        self.assertIn('401', str(context.exception))
    
    @patch('tradestation.api.client.requests')
    def test_successful_request_no_retry(self, mock_requests):
        """Test that successful requests don't trigger retry logic."""
        # Successful response
        success_response = Mock()
        success_response.ok = True
        success_response.json = Mock(return_value={'balance': 10000})
        
        mock_requests.get = Mock(return_value=success_response)
        
        # Make request
        result = self.client.get('/v3/test')
        
        # Verify no retry (only 1 call)
        self.assertEqual(mock_requests.get.call_count, 1, "Should not retry successful requests")
        self.client.auth.clear_token_cache.assert_not_called()
        self.assertEqual(result, {'balance': 10000})


if __name__ == '__main__':
    unittest.main()
