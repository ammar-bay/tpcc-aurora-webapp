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
        Execute TPC-C New Order transaction.
        
        Args:
            warehouse_id (int)
            district_id (int)
            customer_id (int)
            items (list of dict): [{'item_id': int, 'quantity': int}]
        
        Returns:
            int: new order ID
        """
        try:
            if not self.connection:
                self._connect()

            with self.connection.cursor() as cur:
                # 1. Get next order ID for district
                cur.execute(
                    """
                    SELECT d_next_o_id FROM district
                    WHERE d_w_id = %s AND d_id = %s
                    FOR UPDATE
                    """,
                    (warehouse_id, district_id)
                )
                d_row = cur.fetchone()
                if not d_row:
                    raise ValueError("District not found")
                next_o_id = d_row["d_next_o_id"]

                # 2. Increment district next order ID
                cur.execute(
                    """
                    UPDATE district
                    SET d_next_o_id = d_next_o_id + 1
                    WHERE d_w_id = %s AND d_id = %s
                    """,
                    (warehouse_id, district_id)
                )

                # 3. Get customer info for discount, credit, etc.
                cur.execute(
                    """
                    SELECT c_discount, c_credit, c_last
                    FROM customer
                    WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
                    """,
                    (warehouse_id, district_id, customer_id)
                )
                customer = cur.fetchone()
                if not customer:
                    raise ValueError("Customer not found")

                o_ol_cnt = len(items)
                o_all_local = 1  # assuming all items from local warehouse

                # 4. Insert into orders
                cur.execute(
                    """
                    INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, NULL, %s, %s)
                    """,
                    (next_o_id, district_id, warehouse_id, customer_id, o_ol_cnt, o_all_local)
                )

                # 5. Insert into new_order
                cur.execute(
                    """
                    INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
                    VALUES (%s, %s, %s)
                    """,
                    (next_o_id, district_id, warehouse_id)
                )

                # 6. Process each item
                for line_number, item in enumerate(items, start=1):
                    item_id = item["item_id"]
                    quantity = item["quantity"]

                    # Get item price
                    cur.execute(
                        "SELECT i_price FROM item WHERE i_id = %s",
                        (item_id,)
                    )
                    item_row = cur.fetchone()
                    if not item_row:
                        raise ValueError(f"Item {item_id} not found")
                    price = item_row["i_price"]

                    # Update stock
                    cur.execute(
                        """
                        SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt
                        FROM stock
                        WHERE s_w_id = %s AND s_i_id = %s
                        FOR UPDATE
                        """,
                        (warehouse_id, item_id)
                    )
                    stock = cur.fetchone()
                    if not stock:
                        raise ValueError(f"Stock for item {item_id} not found")

                    new_qty = stock["s_quantity"] - quantity
                    if new_qty < 10:
                        new_qty += 100  # TPC-C wrap-around

                    cur.execute(
                        """
                        UPDATE stock
                        SET s_quantity = %s,
                            s_ytd = s_ytd + %s,
                            s_order_cnt = s_order_cnt + 1
                        WHERE s_w_id = %s AND s_i_id = %s
                        """,
                        (new_qty, quantity, warehouse_id, item_id)
                    )

                    # Insert into order_line
                    ol_amount = quantity * price
                    cur.execute(
                        """
                        INSERT INTO order_line (
                            ol_o_id, ol_d_id, ol_w_id, ol_number,
                            ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)
                        """,
                        (
                            next_o_id, district_id, warehouse_id, line_number,
                            item_id, warehouse_id, quantity, ol_amount, "S_DIST_INFO"
                        )
                    )

                # 7. Commit transaction
                self.connection.commit()
                logger.info(f"New order {next_o_id} created for customer {customer_id}")
                return next_o_id

        except Exception as e:
            logger.error(f"Failed to execute new order: {str(e)}")
            if self.connection:
                self.connection.rollback()
            raise