"""
Amazon Aurora DSQL Database Connector - STUDY IMPLEMENTATION SKELETON
Participants will implement this connector to integrate with Aurora DSQL

This file contains TODO items that participants need to complete during the study.
"""

import logging
import os
import select
import time
from typing import Any, Dict, List, Optional
import boto3

import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError, sql
import re

DDL_PATTERN = re.compile(
    r"^\s*(CREATE|ALTER|DROP|TRUNCATE|RENAME|GRANT|REVOKE)", re.IGNORECASE
)
DML_PATTERN = re.compile(r"^\s*(INSERT|UPDATE|DELETE)", re.IGNORECASE)

from .base_connector import BaseDatabaseConnector

logger = logging.getLogger(__name__)


class AuroraDSQLConnector(BaseDatabaseConnector):
    """
    Amazon Aurora DSQL database connector for TPC-C application

    Participants will implement connection management and query execution
    for Aurora DSQL during the UX study.
    """

    def __init__(self):
        """
        Initialize Aurora DSQL connection

        TODO: Implement Aurora DSQL connection initialization
        - Read configuration from environment variables
        - Set up AWS authentication and Aurora DSQL client
        - Configure database connection parameters
        - Handle AWS credentials and region settings

        Environment variables to use:
        - AWS_REGION: AWS region for Aurora DSQL cluster
        - DSQL_CLUSTER_ENDPOINT: Aurora DSQL cluster endpoint
        - AWS_ACCESS_KEY_ID: AWS access key (or use IAM roles)
        - AWS_SECRET_ACCESS_KEY: AWS secret key (or use IAM roles)
        """
        super().__init__()
        self.provider_name = "Amazon Aurora DSQL"
        # TODO: Initialize Aurora DSQL connection
        self.connection = None

        # TODO: Read configuration from environment
        self.region = os.getenv("AWS_REGION")
        self.cluster_endpoint = os.getenv("DSQL_CLUSTER_ENDPOINT")
        self.db_name = os.getenv("DSQL_DATABASE", "postgres")
        self.user = os.getenv("DSQL_USERNAME", "admin")
        self.password = os.getenv(
            "DSQL_PASSWORD"
        )  # For IAM auth, this would be a token

        # Validate required configuration
        missing_vars = [
            var
            for var in ["AWS_REGION", "DSQL_CLUSTER_ENDPOINT"]
            if not os.getenv(var)
        ]
        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

        logger.info(
            f"Initializing Aurora DSQL connector for endpoint: {self.cluster_endpoint}"
        )

        # Establish connection immediately
        self._connect()

        # TODO: Initialize Aurora DSQL client and connection
        
    def _get_aurora_dsql_token(self):
        dsql = boto3.client('dsql', region_name=self.region)
        
        token = dsql.generate_db_connect_admin_auth_token(
            Hostname=self.cluster_endpoint,     # e.g. "xyz.dsql.us-west-2.on.aws"
            Region=self.region,                 # or omit to default to client region
            ExpiresIn=3600                      # optional: duration in seconds (max: 604800 = 1 week)
        )
        return token

    def _connect(self):
        """
        Test connection to Aurora DSQL database

        TODO: Implement connection testing
        - Test connection to Aurora DSQL cluster
        - Execute a simple query to verify connectivity
        - Return True if successful, False otherwise
        - Log connection status for study data collection

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            start_time = time.time()
            token = self._get_aurora_dsql_token()
            self.connection = psycopg2.connect(
                host=self.cluster_endpoint,
                user=self.user,
                password=token,
                # password=self.password,
                dbname=self.db_name,
                port=5432,
                connect_timeout=10,
                sslmode="require",  # DSQL requires SSL
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
            elapsed = time.time() - start_time
            logger.info(f"Connected to Aurora DSQL in {elapsed:.2f}s")
        except OperationalError as e:
            logger.error(f"Failed to connect to Aurora DSQL: {str(e)}")
            raise

    def test_connection(self) -> bool:
        """
        Test connection to Aurora DSQL database by executing a simple SELECT query.
        """
        try:
            if not self.connection:
                self._connect()

            with self.connection.cursor() as cur:
                cur.execute("SELECT 1 AS test")
                result = cur.fetchone()
                if result and result["test"] == 1:
                    logger.info("Aurora DSQL connection test successful")
                    return True
            return False
        except Exception as e:
            logger.error(f"Aurora DSQL connection test failed: {str(e)}")
            return False

    def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL query on Aurora DSQL and return results as a list of dicts.
        Enforces Aurora DSQL transaction constraints:
        - No mixing of DDL and DML in a transaction
        - Only 1 DDL per transaction
        - Max 3,000 rows modified per transaction
        """
        try:
            if not self.connection:
                self._connect()

            is_ddl = bool(DDL_PATTERN.match(query))
            is_dml = bool(DML_PATTERN.match(query))

            start_time = time.time()
            rows = []

            with self.connection.cursor() as cur:
                if is_ddl:
                    # DDL must be in its own transaction
                    logger.debug("Executing DDL in its own transaction")
                    cur.execute(query, params or ())
                    self.connection.commit()
                elif is_dml:
                    # Optionally enforce row limit — relies on LIMIT clause in query
                    # if "limit" not in query.lower():
                    #     logger.warning(
                    #         "DML detected without explicit LIMIT — may exceed 3,000-row limit"
                    #     )
                    cur.execute(query, params or ())
                    self.connection.commit()
                    if cur.description:
                        rows = cur.fetchall()
                else:
                    # Read-only or other statements
                    cur.execute(query, params or ())
                    if cur.description:
                        rows = cur.fetchall()

            elapsed = time.time() - start_time
            logger.debug(f"Executed query in {elapsed:.2f}s: {query}")
            return rows

        except Exception as e:
            logger.error(
                f"Aurora DSQL query execution failed: {str(e)}\nQuery: {query}"
            )
            if self.connection:
                try:
                    self.connection.rollback()
                except Exception as rollback_err:
                    logger.warning(f"Rollback failed: {rollback_err}")
            raise

    def get_provider_name(self) -> str:
        """Return the provider name"""
        return self.provider_name

    def close_connection(self):
        """
        Close the database connection.
        """
        try:
            if self.connection:
                self.connection.close()
                logger.info("Aurora DSQL connection closed")
                self.connection = None
        except Exception as e:
            logger.error(f"Connection cleanup failed: {str(e)}")

    def execute_new_order(
        self,
        warehouse_id: int,
        district_id: int,
        customer_id: int,
        items: List[Dict[str, Any]]
    ) -> int:
        """
        Create a new order in the TPC-C style schema.

        Args:
            warehouse_id (int): Warehouse ID where the order is placed.
            district_id (int): District ID within the warehouse.
            customer_id (int): Customer placing the order.
            items (List[Dict[str, Any]]): List of order items, each dict containing:
                - item_id (int)
                - supply_warehouse_id (int)
                - quantity (int)
                - price (float)

        Returns:
            int: The newly created order ID.
        """
        try:
            if not self.connection:
                self._connect()

            with self.connection.cursor() as cur:
                # 1. Insert into orders table and get order_id
                cur.execute(
                    """
                    INSERT INTO orders (warehouse_id, district_id, customer_id, entry_date)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    RETURNING order_id
                    """,
                    (warehouse_id, district_id, customer_id)
                )
                order_id = cur.fetchone()["order_id"]

                # 2. Insert order lines
                for line_number, item in enumerate(items, start=1):
                    cur.execute(
                        """
                        INSERT INTO order_line (
                            order_id, line_number, item_id,
                            supply_warehouse_id, quantity, price, amount
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            order_id,
                            line_number,
                            item["item_id"],
                            item["supply_warehouse_id"],
                            item["quantity"],
                            item["price"],
                            item["quantity"] * item["price"]
                        )
                    )

                self.connection.commit()
                logger.info(f"New order {order_id} created with {len(items)} items.")
                return order_id

        except Exception as e:
            logger.error(f"Failed to execute new order: {str(e)}")
            if self.connection:
                self.connection.rollback()
            raise