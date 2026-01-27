"""
Quality Checks Module for Medallion Data Warehouse

This module provides configurable data quality validation for the Medallion architecture.
"""

import logging
import yaml
import json
import csv
import os
from typing import Dict, List, Any, Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QualityCheckError(Exception):
    """Custom exception for quality check failures"""
    pass


class DataQualityChecker:
    """Handles data quality checks for Medallion layers"""

    def __init__(self, config_path: str = '/opt/project/config/quality_checks.yaml'):
        self.config_path = config_path
        self.config = self._load_config()
        self.pass_threshold = self.config.get('quality_thresholds', {}).get('pass_threshold', 95.0)

    def _load_config(self) -> Dict[str, Any]:
        """Load quality check configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded quality check configuration from {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config from {self.config_path}: {e}")
            raise QualityCheckError(f"Configuration loading failed: {e}")

    def run_quality_checks(self, layer: str, postgres_conn_id: str = 'postgres_default') -> Dict[str, Any]:
        """
        Run all quality checks for a given layer

        Args:
            layer: 'silver' or 'gold'
            postgres_conn_id: Airflow connection ID for PostgreSQL

        Returns:
            Dict containing summary of all quality check results
        """
        if layer not in ['silver', 'gold']:
            raise ValueError(f"Invalid layer: {layer}. Must be 'silver' or 'gold'")

        logger.info(f"Starting quality checks for {layer} layer")
        checks = self.config.get('quality_checks', {}).get(layer, [])

        if not checks:
            logger.warning(f"No quality checks defined for {layer} layer")
            return {}

        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        summary = {}

        try:
            conn = hook.get_conn()
            cursor = conn.cursor()

            for check in checks:
                check_name = check['name']
                logger.info(f"Running quality check: {check_name}")

                try:
                    result = self._run_single_check(cursor, check, layer)
                    summary[check_name] = result

                    # Log results
                    status = result['status']
                    clean_pct = result['clean_percentage']
                    logger.info(f"Check {check_name}: {status} ({clean_pct:.2f}% clean)")

                except Exception as e:
                    logger.error(f"Quality check {check_name} failed: {e}")
                    summary[check_name] = {
                        'total_records': 0,
                        'issues_found': 0,
                        'clean_records': 0,
                        'clean_percentage': 0.0,
                        'issue_percentage': 0.0,
                        'status': 'ERROR',
                        'error': str(e)
                    }

        except Exception as e:
            logger.error(f"Failed to execute quality checks for {layer}: {e}")
            raise AirflowException(f"Quality checks failed: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

        # Overall assessment
        overall_status = self._calculate_overall_status(summary)
        summary['_overall'] = {
            'total_checks': len([k for k in summary.keys() if not k.startswith('_')]),
            'passed_checks': sum(1 for r in summary.values() if isinstance(r, dict) and r.get('status') == 'PASS'),
            'failed_checks': sum(1 for r in summary.values() if isinstance(r, dict) and r.get('status') == 'FAIL'),
            'error_checks': sum(1 for r in summary.values() if isinstance(r, dict) and r.get('status') == 'ERROR'),
            'overall_status': overall_status
        }

        # Generate reports after overall assessment
        self._generate_reports(layer, summary)

        logger.info(f"Completed quality checks for {layer} layer. Overall status: {overall_status}")
        return summary

    def _run_single_check(self, cursor, check: Dict[str, Any], layer: str) -> Dict[str, Any]:
        """Run a single quality check"""
        table = check['table']
        issue_query = check['issue_query']
        csv_file = check['csv_file']

        # Get total records
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total = cursor.fetchone()[0]
        except Exception as e:
            raise QualityCheckError(f"Failed to get total count from {table}: {e}")

        # Get issue records
        try:
            cursor.execute(issue_query)
            issues = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            issue_count = len(issues)
        except Exception as e:
            raise QualityCheckError(f"Failed to execute issue query: {e}")

        # Calculate metrics
        clean_count = total - issue_count
        clean_percentage = (clean_count / total * 100) if total > 0 else 0
        issue_percentage = (issue_count / total * 100) if total > 0 else 0

        # Determine status based on severity and threshold
        severity = check.get('severity', 'medium')
        status = self._determine_status(clean_percentage, severity)

        # Save CSV
        try:
            os.makedirs(os.path.dirname(csv_file), exist_ok=True)
            with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(column_names)
                writer.writerows(issues)
        except Exception as e:
            logger.warning(f"Failed to save CSV for {check['name']}: {e}")

        return {
            'total_records': total,
            'issues_found': issue_count,
            'clean_records': clean_count,
            'clean_percentage': round(clean_percentage, 2),
            'issue_percentage': round(issue_percentage, 2),
            'status': status,
            'severity': severity,
            'description': check.get('description', '')
        }

    def _determine_status(self, clean_percentage: float, severity: str) -> str:
        """Determine pass/fail status based on clean percentage and severity"""
        if clean_percentage >= self.pass_threshold:
            return 'PASS'
        elif severity == 'critical':
            return 'FAIL'
        elif severity == 'high' and clean_percentage < 90.0:
            return 'FAIL'
        elif severity == 'medium' and clean_percentage < 85.0:
            return 'FAIL'
        else:
            return 'WARNING'

    def _calculate_overall_status(self, summary: Dict[str, Any]) -> str:
        """Calculate overall status across all checks"""
        if any(r.get('status') == 'ERROR' for r in summary.values()):
            return 'ERROR'
        elif any(r.get('status') == 'FAIL' for r in summary.values()):
            return 'FAIL'
        elif any(r.get('status') == 'WARNING' for r in summary.values()):
            return 'WARNING'
        else:
            return 'PASS'

    def _generate_reports(self, layer: str, summary: Dict[str, Any]):
        """Generate JSON and text reports"""
        artifacts_dir = '/opt/project/artifacts'

        # JSON summary
        json_file = f"{artifacts_dir}/{layer}_quality_summary.json"
        try:
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            logger.info(f"Generated JSON report: {json_file}")
        except Exception as e:
            logger.error(f"Failed to generate JSON report: {e}")

        # Text report
        txt_file = f"{artifacts_dir}/{layer}_quality_report.txt"
        try:
            with open(txt_file, 'w', encoding='utf-8') as f:
                f.write(f"Quality Report for {layer.upper()} Layer\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Generated: {self._get_timestamp()}\n\n")

                for check_name, metrics in summary.items():
                    if check_name.startswith('_'):
                        continue

                    f.write(f"Check: {check_name}\n")
                    f.write(f"  Description: {metrics.get('description', 'N/A')}\n")
                    f.write(f"  Severity: {metrics.get('severity', 'N/A')}\n")
                    f.write(f"  Total Records: {metrics['total_records']}\n")
                    f.write(f"  Issues Found: {metrics['issues_found']}\n")
                    f.write(f"  Clean Records: {metrics['clean_records']}\n")
                    f.write(f"  Clean Percentage: {metrics['clean_percentage']}%\n")
                    f.write(f"  Issue Percentage: {metrics['issue_percentage']}%\n")
                    f.write(f"  Status: {metrics['status']}\n")

                    if 'error' in metrics:
                        f.write(f"  Error: {metrics['error']}\n")

                    f.write("\n")

                # Overall summary
                overall = summary.get('_overall', {})
                f.write("OVERALL SUMMARY\n")
                f.write("-" * 20 + "\n")
                f.write(f"Total Checks: {overall.get('total_checks', 0)}\n")
                f.write(f"Passed: {overall.get('passed_checks', 0)}\n")
                f.write(f"Failed: {overall.get('failed_checks', 0)}\n")
                f.write(f"Errors: {overall.get('error_checks', 0)}\n")
                f.write(f"Overall Status: {overall.get('overall_status', 'UNKNOWN')}\n")

            logger.info(f"Generated text report: {txt_file}")
        except Exception as e:
            logger.error(f"Failed to generate text report: {e}")

    def _get_timestamp(self) -> str:
        """Get current timestamp for reports"""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Global instance for reuse
quality_checker = None

def get_quality_checker(config_path='/opt/project/config/quality_checks.yaml'):
    """Get or create quality checker instance"""
    global quality_checker
    if quality_checker is None:
        quality_checker = DataQualityChecker(config_path)
    return quality_checker


def run_quality_checks(layer: str, config_path: str = '/opt/project/config/quality_checks.yaml', **kwargs) -> Dict[str, Any]:
    """
    Airflow-compatible function to run quality checks

    Args:
        layer: 'silver' or 'gold'
        config_path: Path to quality check configuration file
        **kwargs: Additional arguments (passed by Airflow)

    Returns:
        Dict containing quality check results
    """
    checker = get_quality_checker(config_path)
    return checker.run_quality_checks(layer)