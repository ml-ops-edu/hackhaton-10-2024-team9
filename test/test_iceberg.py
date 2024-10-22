#!/usr/bin/env python3
import mixins
import unittest
import logging
from contextlib import closing

class TestIceberg(mixins.TestCase):

    _catalog = 'iceberg'

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_0010_list_catalogs(self):
        with closing(self.conn.cursor()) as cur:
            cur.execute("SHOW CATALOGS")
            rows = cur.fetchall()
            self.assertIsNotNone(rows, "SHOW CATALOGS returned None")
            for i in ["system", self.catalog]:
                self.assertIn([i], rows, f"Catalog {i} not found")

    def test_0020_create_schema(self):
        with closing(self.conn.cursor()) as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schemaNm}")
            rows = cur.fetchall()
            self.assertIsNotNone(rows, "CREATE SCHEMA returned None")

    def test_0030_list_schemas(self):
        with closing(self.conn.cursor()) as cur:
            cur.execute(f"SHOW SCHEMAS FROM {self.catalog}")
            rows = cur.fetchall()
            self.assertIsNotNone(rows, "SHOW SCHEMAS returned None")
            for i in [self.schemaNm.lower()]:
                self.assertIn([i], rows, f"Schema {i} not found")

    def test_0040_create_table(self):
        #notes: CHAR(N), JSON, TINYINT, SMALLINT are not supported Iceberg types
        # CHAR(N) does not fail, but it results in inconsistent field lengths and paddings.
        with closing(self.conn.cursor()) as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schemaNm}.table_1(
                  c1  INT,
                  c2  BIGINT,
                  c3  BOOLEAN,
                  c4  REAL,
                  c5  DOUBLE,
                  c6  DECIMAL(20,10),
                  c7  VARCHAR,
                  c8  VARBINARY,
                  c9  ARRAY<INTEGER>,
                  c10 MAP<VARCHAR,INTEGER>,
                  c11 ROW(a VARCHAR, b INT),
                  c12 UUID,
                  c13 TIME(6),
                  c14 TIMESTAMP(6) WITHOUT TIME ZONE,
                  c15 TIMESTAMP(6) WITH TIME ZONE,
                  c16 DATE
                ) WITH (
                  location='s3a://{self.s3Bucket}/trino/data/unittest/{self.schemaNm}/parquet/table_1',
                  format='PARQUET',
                  partitioning=ARRAY['year(c16)']
                )
            """)
            rows = cur.fetchall()
            self.assertIsNotNone(rows, "CREATE TABLE returned None")

    def test_0050_show_tables(self):
        with closing(self.conn.cursor()) as cur:
            cur.execute(f"SHOW TABLES FROM {self.catalog}.{self.schemaNm}")
            rows = cur.fetchall()
            self.assertIsNotNone(rows, "SHOW TABLES returned None")
            for i in ["table_1"]:
                self.assertIn([i], rows, f"Table {i} not found")

    def test_0060_insert_into_table(self):
        import uuid
        with closing(self.conn.cursor()) as cur:
            cur.execute(f"""
                INSERT INTO {self.catalog}.{self.schemaNm}.table_1 VALUES
                (   {self.minInt},
                    {self.minBig},
                    true,
                    0.14285714285714286,
                    0.14285714285714285714,
                    3.14,
                    '{self.range("AZ","az","09")}''{self.symbols}',
                    CAST(from_utf8(x'65683F') AS VARBINARY),
                    ARRAY[1, 2, 3],
                    MAP(ARRAY['foo', 'bar'], ARRAY[1, 2]),
                    ROW('{self.symbols}', 0x07f),
                    UUID '{uuid.uuid4()}',
                    TIME '15:55:23',
                    TIMESTAMP '2024-07-01 15:55:23',
                    TIMESTAMP '2024-07-01 15:55:23.123456 Europe/Zurich',
                    DATE '2024-07-01'),
                (   {self.maxInt},
                    {self.maxBig},
                    TRUE,
                    -.14285714285714285714,
                    -.14285714285714285714,
                    -3.14,
                    '{self.accents}{self.specials}',
                    CAST(from_utf8(x'65683F') AS VARBINARY),
                    ARRAY[1, 2, 3],
                    MAP(ARRAY['foo', 'bar'], ARRAY[1, 2]),
                    ROW('{self.specials}', -2),
                    UUID '{uuid.uuid4()}',
                    TIME '15:55:23',
                    TIMESTAMP '2024-07-01 15:55:23',
                    TIMESTAMP '2024-07-01 15:55:23.123456 GMT',
                    DATE '2024-07-01')""")
            rows = cur.fetchall()
            self.assertIsNotNone(rows, "INSERT INTO table failed")

    def test_0070_select_from_table(self):
        from decimal import Decimal
        from collections import namedtuple
        from datetime import datetime, date
        from zoneinfo import ZoneInfo
        with closing(self.conn.cursor()) as cur:
            cur.execute(f"SELECT * FROM {self.catalog}.{self.schemaNm}.table_1")
            rows = cur.fetchall()
            self.logger.debug(f'test_0070_select_from_table', extra={'nl':True})
            i,j=(0,0)
            for row in rows:
                i+=1
                for value in row:
                    j+=1
                    self.logger.debug(f'[{i:02d},{j:02d}]> {value}')
            self.assertEqual(i, 2, "Did not get the expected number of rows")
            self.assertIsNotNone(rows, "SELECT * FROM table returned None")
            self.assertEqual(rows[0][0], self.minInt)
            self.assertEqual(rows[0][1], self.minBig)
            self.assertEqual(rows[0][2], True)
            self.assertEqual(rows[0][3], .14285715)
            self.assertEqual(rows[0][4], .14285714285714285714)
            self.assertEqual(rows[0][5], Decimal('3.14'))
            self.assertEqual(rows[0][6], f'{self.range("AZ","az","09")}\'{self.symbols}')
            self.assertListEqual(rows[0][8], [1,2,3])
            self.assertDictEqual(rows[0][9], {'foo':1, 'bar':2})
            self.assertTupleEqual(rows[0][10], namedtuple("Row", "a b")(f'{self.symbols}', 0x07f))
            self.assertRegex(str(rows[0][11]), '^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$')
            self.assertEqual(rows[0][13], datetime(2024, 7, 1, 15, 55, 23))
            self.assertEqual(rows[0][14], datetime(2024, 7, 1, 15, 55, 23, 123456, tzinfo=ZoneInfo('Europe/Zurich')))
            self.assertEqual(rows[0][15], date(2024, 7, 1))
            self.assertEqual(rows[1][0], self.maxInt)
            self.assertEqual(rows[1][1], self.maxBig)
            self.assertEqual(rows[1][2], True)
            self.assertEqual(rows[1][3], -.14285715)
            self.assertEqual(rows[1][4], -.14285714285714285714)
            self.assertEqual(rows[1][5], Decimal('-3.14'))
            self.assertEqual(rows[1][6], f'{self.accents}{self.specials}')
            self.assertListEqual(rows[1][8], [1,2,3])
            self.assertDictEqual(rows[1][9], {'foo':1, 'bar':2})
            self.assertTupleEqual(rows[1][10], namedtuple("Row", "a b")(f'{self.specials}', -2))
            self.assertRegex(str(rows[1][11]), '^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$')
            self.assertEqual(rows[1][13], datetime(2024, 7, 1, 15, 55, 23))
            self.assertEqual(rows[1][14], datetime(2024, 7, 1, 15, 55, 23, 123456, tzinfo=ZoneInfo('GMT')))
            self.assertEqual(rows[1][15], date(2024, 7, 1))

if __name__ == '__main__':
    unittest.main()
