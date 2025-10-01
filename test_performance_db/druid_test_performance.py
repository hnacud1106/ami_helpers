import asyncio
import aiohttp
import time
import statistics
from datetime import datetime
from aiomultiprocess import Pool
import multiprocessing


class AioMultiprocessDruidTester:
    def __init__(self, druid_host="100.100.11.1", druid_port=8082):
        self.druid_host = druid_host
        self.druid_port = druid_port
        self.base_url = f"http://{druid_host}:{druid_port}/druid/v2/sql/"

    def create_test_query(self, query_id):
        queries = [
            """SELECT CASE
  WHEN length(brand) > 15
  THEN left(brand, 15) || '...'
  ELSE brand
END AS "brand", "channel_name_tvd" AS "channel_name_tvd", COUNT(DISTINCT user_id) AS "reach" 
FROM "druid"."fact_reachs" 
WHERE "__time" >= '2025-09-17 00:00:00.000000' AND "__time" <= '2025-09-18 23:59:59.000000' GROUP BY CASE
  WHEN length(brand) > 15
  THEN left(brand, 15) || '...'
  ELSE brand
END, "channel_name_tvd" ORDER BY "reach" DESC
 LIMIT 50000""",
            """
            SELECT CASE
  WHEN length(brand) > 15
  THEN left(brand, 15) || '...'
  ELSE brand
END AS "brand", CASE WHEN time_band IN ('11h - 12h', '12h - 13h', '13h - 14h') THEN '2.Trưa' WHEN time_band IN ('14h - 15h', '15h - 16h', '16h - 17h', '17h - 18h') THEN '3.Chiều' WHEN time_band IN ('18h - 19h', '19h - 20h', '20h - 21h', '21h - 22h', '22h - 23h', '23h - 24h') THEN '4.Tối' ELSE '1.Sáng' END AS "time_band", COUNT(DISTINCT user_id) AS "reach" 
FROM "druid"."fact_reachs" 
WHERE "__time" >= '2025-09-17 00:00:00.000000' AND "__time" <= '2025-09-18 23:59:59.000000' GROUP BY CASE
  WHEN length(brand) > 15
  THEN left(brand, 15) || '...'
  ELSE brand
END, CASE WHEN time_band IN ('11h - 12h', '12h - 13h', '13h - 14h') THEN '2.Trưa' WHEN time_band IN ('14h - 15h', '15h - 16h', '16h - 17h', '17h - 18h') THEN '3.Chiều' WHEN time_band IN ('18h - 19h', '19h - 20h', '20h - 21h', '21h - 22h', '22h - 23h', '23h - 24h') THEN '4.Tối' ELSE '1.Sáng' END ORDER BY "reach" DESC
 LIMIT 50000""",
            """SELECT CASE
  WHEN length(brand) > 15
  THEN left(brand, 15) || '...'
  ELSE brand
END AS "brand", "firstlevel_vn" AS "firstlevel_vn", COUNT(DISTINCT user_id) AS "reach" 
FROM "druid"."fact_reachs" 
WHERE NOT ("firstlevel_vn" IS NULL OR "firstlevel_vn" IN (NULL) AND (1 != 1)) AND "__time" >= '2025-09-17 00:00:00.000000' AND "__time" <= '2025-09-18 23:59:59.000000' GROUP BY CASE
  WHEN length(brand) > 15
  THEN left(brand, 15) || '...'
  ELSE brand
END, "firstlevel_vn" ORDER BY "reach" DESC
 LIMIT 50000""",
            """SELECT CASE
  WHEN length(advertiser) > 15
  THEN left(advertiser, 15) || '...'
  ELSE advertiser
END AS "advertiser", COUNT(DISTINCT user_id) AS "reach" 
FROM "druid"."fact_reachs" 
WHERE "advertiser" IS NOT NULL AND "__time" >= '2025-09-17 00:00:00.000000' AND "__time" <= '2025-09-18 23:59:59.000000' GROUP BY CASE
  WHEN length(advertiser) > 15
  THEN left(advertiser, 15) || '...'
  ELSE advertiser
END ORDER BY "reach" DESC
 LIMIT 50000"""
        ]
        return queries[query_id % len(queries)]

    async def execute_query_batch(self, query_batch):
        results = []

        connector = aiohttp.TCPConnector(limit=200, limit_per_host=200)
        semaphore = asyncio.Semaphore(200)

        async with aiohttp.ClientSession(
                connector=connector,
                headers={'Content-Type': 'application/json'}
        ) as session:

            async def single_query(query_id):
                async with semaphore:
                    start_time = time.time()
                    query = self.create_test_query(query_id)

                    payload = {
                        "query": query,
                        "context": {"timeout": 30000}
                    }

                    try:
                        async with session.post(
                                self.base_url,
                                json=payload,
                                timeout=aiohttp.ClientTimeout(total=60)
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
                                    data = await response.json()
                                    result['row_count'] = len(data) if isinstance(data, list) else 0
                                    print(
                                        f"Query ID {query_id} succeeded in {result['response_time_ms']:.2f} ms  - with {result['row_count']} rows.")
                                except:
                                    result['row_count'] = 0
                                    print(
                                        f"Query ID {query_id} failed in {result['response_time_ms']:.2f} ms  - with {result['row_count']} rows.")
                            else:
                                print(
                                    f"Query ID {query_id} failed in {result['response_time_ms']:.2f} ms - Status: {await response.text()}")

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
            print(f"\nTHỐI GIAN PHẢN HỒI (ms):")
            print(f"  Trung bình: {statistics.mean(response_times):.2f}")
            print(f"  Median: {statistics.median(response_times):.2f}")
            print(f"  Min: {min(response_times):.2f}")
            print(f"  Max: {max(response_times):.2f}")
            if len(response_times) > 10:
                print(f"  P95: {sorted(response_times)[int(0.95 * len(response_times))]:.2f}")
                print(f"  P99: {sorted(response_times)[int(0.99 * len(response_times))]:.2f}")


tester_instance = AioMultiprocessDruidTester()


async def execute_query_batch_global(query_batch):
    return await tester_instance.execute_query_batch(query_batch)


async def main():
    if __name__ == "__main__":
        multiprocessing.set_start_method('spawn', force=True)

        global tester_instance
        tester_instance = AioMultiprocessDruidTester(
            druid_host="100.100.11.1",
            druid_port=8082
        )

        await tester_instance.run_multiprocess_test(total_queries=1500, num_processes=6)


if __name__ == "__main__":
    asyncio.run(main())
