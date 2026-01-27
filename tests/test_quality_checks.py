"""
Unit tests for the Data Quality Checker module
"""

import pytest
import yaml
import json
import tempfile
import os
import sys
from unittest.mock import Mock, patch, MagicMock

# Add the dags directory to the path so we can import quality_checks
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'docker', 'dags'))

from quality_checks import DataQualityChecker, QualityCheckError


class TestDataQualityChecker:
    """Test cases for DataQualityChecker class"""

    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing"""
        return {
            'quality_checks': {
                'silver': [
                    {
                        'name': 'test_check',
                        'table': 'silver.test_table',
                        'description': 'Test quality check',
                        'issue_query': 'SELECT * FROM silver.test_table WHERE invalid_column = 1',
                        'csv_file': '/tmp/test.csv',
                        'severity': 'medium'
                    }
                ],
                'gold': []
            },
            'quality_thresholds': {
                'pass_threshold': 95.0,
                'severity_weights': {
                    'critical': 1.0,
                    'high': 0.8,
                    'medium': 0.6,
                    'low': 0.4
                }
            }
        }

    @pytest.fixture
    def temp_config_file(self, sample_config):
        """Create a temporary config file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(sample_config, f)
            return f.name

    def test_init_with_valid_config(self, temp_config_file):
        """Test initialization with valid config file"""
        checker = DataQualityChecker(temp_config_file)
        assert checker.config_path == temp_config_file
        assert 'quality_checks' in checker.config
        assert checker.pass_threshold == 95.0

    def test_init_with_invalid_config(self):
        """Test initialization with invalid config file"""
        with pytest.raises(QualityCheckError):
            DataQualityChecker('/nonexistent/file.yaml')

    @patch('dags.quality_checks.PostgresHook')
    def test_run_quality_checks_success(self, mock_hook_class, temp_config_file):
        """Test successful quality check execution"""
        # Mock database connection and cursor
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = [100]  # Total count
        mock_cursor.fetchall.return_value = [('issue1',), ('issue2',)]  # Issues
        mock_cursor.description = [('column1',), ('column2',)]

        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor

        mock_hook = Mock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        checker = DataQualityChecker(temp_config_file)

        with patch('builtins.open', Mock()) as mock_open, \
             patch('os.makedirs', Mock()) as mock_makedirs:

            result = checker.run_quality_checks('silver')

            # Verify results
            assert 'test_check' in result
            check_result = result['test_check']
            assert check_result['total_records'] == 100
            assert check_result['issues_found'] == 2
            assert check_result['clean_records'] == 98
            assert check_result['clean_percentage'] == 98.0
            assert check_result['status'] == 'PASS'

            # Verify overall summary
            assert '_overall' in result
            overall = result['_overall']
            assert overall['total_checks'] == 1
            assert overall['passed_checks'] == 1
            assert overall['overall_status'] == 'PASS'

    @patch('dags.quality_checks.PostgresHook')
    def test_run_quality_checks_with_error(self, mock_hook_class, temp_config_file):
        """Test quality check execution with database error"""
        mock_hook = Mock()
        mock_hook.get_conn.side_effect = Exception("Database connection failed")
        mock_hook_class.return_value = mock_hook

        checker = DataQualityChecker(temp_config_file)

        with pytest.raises(Exception):  # Should raise AirflowException
            checker.run_quality_checks('silver')

    def test_determine_status_pass(self, temp_config_file):
        """Test status determination for passing checks"""
        checker = DataQualityChecker(temp_config_file)

        assert checker._determine_status(98.0, 'medium') == 'PASS'
        assert checker._determine_status(95.0, 'high') == 'PASS'

    def test_determine_status_fail(self, temp_config_file):
        """Test status determination for failing checks"""
        checker = DataQualityChecker(temp_config_file)

        assert checker._determine_status(90.0, 'critical') == 'FAIL'
        assert checker._determine_status(80.0, 'high') == 'FAIL'
        assert checker._determine_status(70.0, 'medium') == 'FAIL'

    def test_calculate_overall_status(self, temp_config_file):
        """Test overall status calculation"""
        checker = DataQualityChecker(temp_config_file)

        # All pass
        summary = {
            'check1': {'status': 'PASS'},
            'check2': {'status': 'PASS'}
        }
        assert checker._calculate_overall_status(summary) == 'PASS'

        # Has failure
        summary = {
            'check1': {'status': 'PASS'},
            'check2': {'status': 'FAIL'}
        }
        assert checker._calculate_overall_status(summary) == 'FAIL'

        # Has error
        summary = {
            'check1': {'status': 'PASS'},
            'check2': {'status': 'ERROR'}
        }
        assert checker._calculate_overall_status(summary) == 'ERROR'

    @patch('dags.quality_checks.PostgresHook')
    def test_run_single_check_error_handling(self, mock_hook_class, temp_config_file):
        """Test error handling in single check execution"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Query failed")

        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor

        mock_hook = Mock()
        mock_hook.get_conn.return_value = mock_conn
        mock_hook_class.return_value = mock_hook

        checker = DataQualityChecker(temp_config_file)

        with pytest.raises(QualityCheckError):
            checker._run_single_check(mock_cursor, checker.config['quality_checks']['silver'][0], 'silver')

    def test_invalid_layer(self, temp_config_file):
        """Test invalid layer validation"""
        checker = DataQualityChecker(temp_config_file)

        with pytest.raises(ValueError):
            checker.run_quality_checks('invalid_layer')


def test_run_quality_checks_function():
    """Test the standalone function wrapper"""
    with patch('dags.quality_checks.quality_checker') as mock_checker:
        mock_checker.run_quality_checks.return_value = {'test': 'result'}

        from dags.quality_checks import run_quality_checks
        result = run_quality_checks('silver')

        mock_checker.run_quality_checks.assert_called_once_with('silver')
        assert result == {'test': 'result'}