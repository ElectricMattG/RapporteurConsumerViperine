#!/usr/bin/env python3
"""
Send URLs to Web Scraper Pipeline

Sends a batch of URLs to the web-scrape-requests Kafka topic for processing.
"""

import json
from datetime import datetime

from confluent_kafka import Producer


def send_urls(urls, bootstrap_servers='localhost:9092', topic='web-scrape-requests'):
    """Send batch of URLs to Kafka pipeline"""

    # Generate session ID
    session_id = f"url-batch-{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Configure producer
    config = {
        'bootstrap.servers': bootstrap_servers
    }

    producer = Producer(config)

    print("=" * 80)
    print(f"📤 Sending {len(urls)} URLs to Web Scraper Pipeline")
    print("=" * 80)
    print(f"Topic: {topic}")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print(f"Session ID: {session_id}")
    print("=" * 80)

    sent_count = 0
    failed_count = 0

    for idx, url in enumerate(urls, 1):
        try:
            # Create message
            message = {
                "uri": url,
                "title": f"Document {idx}",
                "context": [f"Processing URL {idx} of {len(urls)}"],
                "session_id": session_id,
                "request_id": f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{idx:03d}",
                "total_requests": len(urls)
            }

            # Serialize and send
            message_bytes = json.dumps(message).encode('utf-8')
            producer.produce(topic, message_bytes)

            print(f"\n✅ [{idx}/{len(urls)}] Queued: {url}")

            sent_count += 1

        except Exception as e:
            print(f"\n❌ [{idx}/{len(urls)}] Failed: {url}")
            print(f"   Error: {e}")
            failed_count += 1

    # Flush all messages
    print("\n" + "=" * 80)
    print("⏳ Flushing messages to Kafka...")
    producer.flush()

    # Summary
    print("=" * 80)
    print("📊 Summary:")
    print(f"   ✅ Sent: {sent_count}")
    print(f"   ❌ Failed: {failed_count}")
    print(f"   📝 Session ID: {session_id}")
    print("=" * 80)

    return session_id


if __name__ == "__main__":
    # URLs to process - from Vertex AI grounding metadata
    urls = [
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEBbp3nI9smvd6QdzOehRa6hE0vQRwchJ43dumAtQwD4lXRRdtEthighp5OOoiGvDZIHUxWso_g54MyKhjnLOLuyLJWWD42FLCz1Qtcd_WNnZGXNepf9jzqQ41S3BIOuWu-zfTcJahIFBzj7KhSNoBbiF-hpeqMlcoByeEXjMtQ5FrRkSxiSmH-6XG2_pnv6aKOGcR8B82C",
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQH_93xldxtKpcsxLK9YA8oUBX5jpcwrZavlK8IgI86PW28XfxRmRhhi6U4-2_c7pf_sO6QKXf3ebYrLzvdzNRgmUa2kMYoN47JOi92lwrKjPxiA2TST-YVK75AmyXVYBEqGN6NrJIt-ZgQHxiZUkn0SEct65rwFMcHqoQNvrgR2YZpDrjSyI4CPCUiqs26TQoNvIHI9iT5g7npRTslTnkcXuKuu8w==",
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEhwkeQPIrJB8h8Yo5xUDL1sUIaDTtFSFNOO542TDO3OGjEeh1hRrEoqWn4GNRs0jhI2o-0L3Pbxa5S7PiFZx2a7QdowsI0TDiRtR-ln46ZfT9asrynWhGVn47ESzylSl0pxCVwmR8m-QUXPMRyxzce778HbeYDEMn9tO4RFA==",
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQGr4D1T07lA8e4En6K7OKx1Xpp0gZ2N5hzo9g6ORMQuKohUUVaMAK-RtSYuVmInX0Jc0UfNbVe7VksUm8VsGFkYCYBfsAge8gB_Ozk4OVHvHPu5bXwi5yElDc4RfQOmtRFetocQi1uFPEndQey49CHiwb9qWfMmvy9wsxgdsR2sEq-A8Ar6UgkC0uQ329yT7EyVfGDgehw=",
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQE6k1uch1B6vcOu4_NRzUzqMeMNH3T4Z1VkF9VP7PoFL2rP48Pmv-aSFFMSIvjh3WxpPcvorNj4NcH7-OF4vxZk0nAZWY4I-gvnlBShRwGt81i3sNik3t6J6pbiuUlpKTYrEAmY6xaDmTuMlm3L",
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQGjcpFvWDQY_F3pmvGkLD6mj4HOgb5cw7dfCw7nmvteh5r994w8XxaRvCV8GgYeJye-f9RztychAadWjgVg5zdJPJ8enA8qx7lqgdKwA1ETF4BGFiSmtNMf1a-3lwrH93iiXEeLpGgdqDOtO14W5XySYt21aFNhp8QPV8C8a1ANYltYUssFfOfzCxA=",
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHspXnM-uXG0RUkGdjV6mImUw0ESCeFHYTGh0vNAmp-G7tAXPwIIm--jrGI3lfTWNgmObry8a7qI37caW5mwxPS59jFM3wN0DUA5ND0jt9B_XZFVczZI8tWZCROLNLAyCYxLcLEnbIwZhB01HE3x6c6rvNUonBf67VBHvdJmujFGnFrn3x2X69EfBi9H8MRZKy6-SMnj1xBy2SKb50w-7erhMuDQsHwuNROeQ6Y8cRIjULoQ2aYUVUbQA1snXdJduh0e7F-8xo=",
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEyk2nC_1QRdvfu4Lp766PfqeY3-gadvblCQkRCVxL7B-n1wnQIQ2MoTmXyDelN1C1hq26hdyUdfLngC0BLewHlTgLGuvis8HKhIFnbqib-VgMTWmScEIgvil2xt-FDk0kLEcqpYLpLycO45ZtIrzy4Lz50zeekpymUGcpHGFErBiRZMeYv2YMWU87Z_y1UPAvV8Op8-2U0ftrRfKkEc_nztjVwa8qAQYxgMscyUahYFdkdw_Jm_y8DQg==",
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHt24IMVxW76IC-LNjQ0tPsx34l-_AKqeS0za_MbE3wHGSWHGuLWWMN36JZWT_ZBLtSviMXngWEmOwRRP-3O8nzXGuc6HW65z8Dz7aZYhpZlMU8FDQjpbCOyEhvaD57lye-jN5RdJPFvkcpCWxwoOp0LzWrT8d9M_R7OiG-v3nlII1Ryx4VX_Bsgw==",
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEgkR8FTSDnbcveAjkkcIp7N-uHkQC8CNhQSn4VYr_4B-UuB4e0-2AktpmoWzxTDchC6St_ACzxg9NXgRAil_lVdeuac_MVD5WG-Dblj86iKWv5FfsJdIy2RmpJ4BpZT4zsjD__56hLHaB3NzSuKYwU6uERW5Wx4mnzbQPU_9AO5LqtdG5dsVvSxjMAjtdAAB-8LTKNn62tWgn08rHwyZIb6x5ZXtbAUuH84nvf-DjnDf8k0oQmeTukEV0aFs30zw==",
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEa4dr5tj_WVy215kDeRvtJF5nERMxeecVzJLgbjQ_VnUlzK7h5F9TW9-sNE3O8MOtqv4Bfq8e557IXg2QUZr5Tfr0EHeS5F_lxB2wjbQKSIPHQ3CmVWnGTwuahFoK0iSg5jGkgoAeWPoOUook1eOgvkDW5VPhCow9VMF5PEAlH8S46Xi9PNhxw0nfRfczrSxAw"
    ]

    session_id = send_urls(urls)

    print(f"\n✨ All URLs sent! Monitor logs to see processing:")
    print(f"   Web Scraper: tail -f logs/web_scraper.log")
    print(f"   Document Chunker: tail -f logs/kafka_chunker.log")
