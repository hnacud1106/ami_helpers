import asyncio
import aiohttp
import time
import statistics
from datetime import datetime
from aiomultiprocess import Pool
import multiprocessing
from typing import Optional


class AioMultiprocessClickHouseTester:
    def __init__(
        self,
        ch_host: str = "100.100.11.1",
        ch_port: int = 8123,
        database: str = "default",
        user: Optional[str] = None,
        password: Optional[str] = None,
        time_column: str = "__time",
        use_default_format_param: bool = True,
        max_concurrency: int = 200,
    ):
        self.ch_host = ch_host
        self.ch_port = ch_port
        self.database = database
        self.user = user
        self.password = password
        self.time_column = time_column
        self.base_url = f"http://{ch_host}:{ch_port}/"
        self.use_default_format_param = use_default_format_param
        self.max_concurrency = max_concurrency

    def create_test_query(self, query_id: int) -> str:
        brand_expr = """CASE
    WHEN lengthUTF8(brand) > 15 THEN concat(substring(brand, 1, 15), '...')
    ELSE brand
END"""

        advertiser_expr = """CASE
    WHEN lengthUTF8(advertiser) > 15 THEN concat(substring(advertiser, 1, 15), '...')
    ELSE advertiser
END"""

        time_filter = f"""{self.time_column} BETWEEN '2025-07-01' AND '2025-09-29'"""

        queries = [
            f"""
SELECT
  {brand_expr} AS brand,
  channel_name_tvd AS channel_name_tvd,
  count(DISTINCT user_id) AS reach
FROM {self.database}.event_vtv_ratings
WHERE {time_filter}
GROUP BY brand, channel_name_tvd
ORDER BY reach DESC
LIMIT 50000
FORMAT JSON
""",
            f"""
SELECT
  {brand_expr} AS brand,
  CASE
    WHEN time_band IN ('11h - 12h','12h - 13h','13h - 14h') THEN '2.Trưa'
    WHEN time_band IN ('14h - 15h','15h - 16h','16h - 17h','17h - 18h') THEN '3.Chiều'
    WHEN time_band IN ('18h - 19h','19h - 20h','20h - 21h','21h - 22h','22h - 23h','23h - 24h') THEN '4.Tối'
    ELSE '1.Sáng'
  END AS time_band,
  count(DISTINCT user_id) AS reach
FROM {self.database}.event_vtv_ratings
WHERE {time_filter}
GROUP BY brand, time_band
ORDER BY reach DESC
LIMIT 50000
FORMAT JSON
""",
            f"""
SELECT
  {brand_expr} AS brand,
  firstlevel_vn AS firstlevel_vn,
  count(DISTINCT user_id) AS reach
FROM {self.database}.event_vtv_ratings
WHERE firstlevel_vn IS NOT NULL
  AND {time_filter}
GROUP BY brand, firstlevel_vn
ORDER BY reach DESC
LIMIT 50000
FORMAT JSON
""",
            f"""
SELECT
  {advertiser_expr} AS advertiser,
  count(DISTINCT user_id) AS reach
FROM {self.database}.event_vtv_ratings
WHERE advertiser IS NOT NULL
  AND {time_filter}
GROUP BY advertiser
ORDER BY reach DESC
LIMIT 50000
FORMAT JSON
""",
        ]
        return queries[query_id % len(queries)]

    async def execute_query_batch(self, query_batch):
        results = []

        connector = aiohttp.TCPConnector(limit=self.max_concurrency, limit_per_host=self.max_concurrency)
        semaphore = asyncio.Semaphore(self.max_concurrency)

        auth = aiohttp.BasicAuth(self.user, self.password) if self.user else None

        params = {
            "database": self.database,
        }
        if self.use_default_format_param:
            params["default_format"] = "JSON"

        headers = {
            "Content-Type": "text/plain; charset=UTF-8",
        }

        async with aiohttp.ClientSession(
            connector=connector,
            auth=auth,
            headers=headers
        ) as session:

            async def single_query(query_id):
                async with semaphore:
                    start_time = time.time()
                    query = self.create_test_query(query_id)
                    payload_sql = query

                    try:
                        async with session.post(
                            self.base_url,
                            params=params,
                            data=payload_sql.encode("utf-8"),
                            timeout=aiohttp.ClientTimeout(total=6000)
                        ) as response:
                            end_time = time.time()

                            result = {
                                'query_id': query_id,
                                'status_code': response.status,
                                'response_time_ms': (end_time - start_time) * 1000,
                                'success': response.status == 200,
                                'timestamp': datetime.now().isoformat()
                            }

                            if response.status == 200:
                                try:
                                    data = await response.json(content_type=None)
                                    if isinstance(data, dict) and "data" in data:
                                        result['row_count'] = len(data.get("data", []))
                                    elif isinstance(data, dict) and "rows" in data:
                                        result['row_count'] = int(data.get("rows", 0))
                                    else:
                                        result['row_count'] = 0
                                    print(
                                        f"Query ID {query_id} succeeded in {result['response_time_ms']:.2f} ms  - with {result['row_count']} rows.")
                                except Exception as pe:
                                    txt = await response.text()
                                    result['row_count'] = 0
                                    print(
                                        f"Query ID {query_id} parse-failed in {result['response_time_ms']:.2f} ms - Body starts: {txt[:200]}")
                            else:
                                body = await response.text()
                                print(
                                    f"Query ID {query_id} failed in {result['response_time_ms']:.2f} ms - Status: {response.status} - {body[:200]}")

                            return result

                    except Exception as e:
                        print(
                            f"Query ID {query_id} failed - Error: {str(e)}")
                        return {
                            'query_id': query_id,
                            'status_code': 0,
                            'response_time_ms': (time.time() - start_time) * 1000,
                            'success': False,
                            'timestamp': datetime.now().isoformat(),
                            'error': str(e)[:200],
                            'row_count': 0
                        }

            tasks = [single_query(qid) for qid in query_batch]
            results = await asyncio.gather(*tasks)

        return results

    async def run_multiprocess_test(self, total_queries=1500, num_processes=6):
        print(f"Bắt đầu aiomultiprocess load test với {total_queries} queries trên {num_processes} processes...")
        print("-" * 60)

        start_time = time.time()

        queries_per_process = total_queries // num_processes
        query_batches = []

        for i in range(num_processes):
            start_idx = i * queries_per_process
            end_idx = start_idx + queries_per_process
            if i == num_processes - 1:
                end_idx = total_queries

            query_batches.append(list(range(start_idx, end_idx)))

        async with Pool(processes=num_processes) as pool:
            batch_results = await pool.map(self.execute_query_batch, query_batches)

        all_results = []
        for batch_result in batch_results:
            all_results.extend(batch_result)

        end_time = time.time()
        total_time = end_time - start_time

        self.analyze_results(all_results, total_time, total_queries)

    def analyze_results(self, results, total_time, num_queries):
        successful_queries = [r for r in results if r['success']]
        failed_queries = [r for r in results if not r['success']]

        response_times = [r['response_time_ms'] for r in successful_queries]

        print("\n" + "=" * 60)
        print("KẾT QUẢ AIOMULTIPROCESS LOAD TEST")
        print("=" * 60)
        print(f"Tổng thời gian test: {total_time:.2f} giây")
        print(f"Tổng số truy vấn: {num_queries}")
        print(f"Truy vấn thành công: {len(successful_queries)}")
        print(f"Truy vấn thất bại: {len(failed_queries)}")
        print(f"Tỷ lệ thành công: {len(successful_queries) / num_queries * 100:.2f}%")
        print(f"Queries per second (QPS): {num_queries / total_time:.2f}")

        if response_times:
            print(f"\nTHỜI GIAN PHẢN HỒI (ms):")
            print(f"  Trung bình: {statistics.mean(response_times):.2f}")
            print(f"  Median: {statistics.median(response_times):.2f}")
            print(f"  Min: {min(response_times):.2f}")
            print(f"  Max: {max(response_times):.2f}")
            if len(response_times) > 10:
                print(f"  P95: {sorted(response_times)[int(0.95 * len(response_times))]:.2f}")
                print(f"  P99: {sorted(response_times)[int(0.99 * len(response_times))]:.2f}")


tester_instance = AioMultiprocessClickHouseTester()


async def execute_query_batch_global(query_batch):
    return await tester_instance.execute_query_batch(query_batch)


async def main():
    if __name__ == "__main__":
        multiprocessing.set_start_method('spawn', force=True)

        global tester_instance
        tester_instance = AioMultiprocessClickHouseTester(
            ch_host="100.100.11.1",
            ch_port=8123,
            database="vtv_ratings",
            user="default",
            password="ChangeMe_Strong_#2025!",
            time_column="date",
            use_default_format_param=True,
            max_concurrency=8
        )

        await tester_instance.run_multiprocess_test(total_queries=10, num_processes=8)


if __name__ == "__main__":
    asyncio.run(main())
