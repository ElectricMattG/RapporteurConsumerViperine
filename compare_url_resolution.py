#!/usr/bin/env python3
"""Compare how the same input URLs resolved differently between sessions."""

import json

# Original citations from test_batch_oct21_28.py
citations = [
    {
        "index": 0,
        "title": "www.gov.il",
        "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEq6Tzk2qEVtfIO6bdBz71cuMTtgAdgpDEAdk4vCsMRX4H82bMgJHi03kJ_K7JFMtbrtTP0IxUrtCmYRYfW6Vdaj3e_0bOCmZx9dtdVHb6mb8vS7ryOG1aHox7cz8GXrH_h_y4JGaVQQywijV4="
    },
    {
        "index": 1,
        "title": "www.gov.il",
        "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQG6mwgF1PTLAyS3DxEis6ShPNPodgSoxi4miq7z4fsqBKc6DKo-zBrwr59tUmFhRYmrtxETK5whfdnnDIqZqj9ffXW1Z1-yk-x5_vCgL7tXHHyXEMAF_9VIge1esH1riflPXORlSBFDWc0PG36bmTaWdHqrAzRY1uKhCBwn"
    },
    {
        "index": 2,
        "title": "timesofisrael.com",
        "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEfbyLCzt72j8Qg9N_dMyFYAc9SDF9pmPwlLhcdpENTHTv2ca1A8-W1z7HvDA3jUyJubPhqO9TeKRzmkkLVP7GGxwmTBoVJcTggUOPgTyl5uiZhufXnbKKkeLcwUfSpQ3COo1psR_AWon61weMHz3QkUNpLxDIE37La5-ese22KaKYLP9Sfhob0nqooBlj9KUtjX43T1usRWsejAiN_rxROnTk1ivFoYw=="
    },
    {
        "index": 3,
        "title": "aljazeera.com",
        "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQGcdes0DXbJpHwt_KEKqNV3SveSGF-mK-jiUQbh5TTnZW8GFk_gMilMB1k-ZrfvKKedmYcqpDohZME7bewdFd_F1a9kX5T6REs6XRo5vYz0WpigpRN2J8xodiAdf-zmpkFF88DvoPoRwlOzEAML1DeCxL3PWONWSD1mVsUVSxYzDpchm7kXQHhyOgsDFwTaIg594rg2oSLaVsBBDtytd5K0D0YFx__m"
    },
    {
        "index": 4,
        "title": "theguardian.com",
        "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHwS5z7pLMiA3xL74Nr2YssbktSCxBT3hRkSWwIxsrRJ8I1yYNaPM5LlO8bzv9223dLm6oSLm-rvwcUhQjyAyOyFWn9QIzmYoWC9yNwJk7s9Dom3iLdZJ0UPs9x1_wVgi5Upk27a-tpD3WqcPPbze4qFQeuiwtpOKyT3Dxy1pr0VBk6fICG2eSh0ISDb-5fRwB0lVdMxNLTP3ed_E5BXw-PoHuCo0TkK6n9IEOvA4s6TMk="
    },
    {
        "index": 5,
        "title": "timesofisrael.com",
        "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFyO3ZKC3V7gEsScKa6jB_w6gac6ntwOkxKrSI27wWg6Dq9K9XcsMQTIb8L4QdGcqyyXiRlKqFeasG0Akt4oroL0_jmkBPEBh2eeqr_Fbv_kv18srKhBZd_6ryEZ6Zi1zM3jnx8K3jaauUsF9GBLAoqegPBa1MHLTNVvJt5HfbpvOKr6gzKsOIUkeyvfm1Vw0kVL4_6c9lr1NE2Lf2TrW7hY3Br4Q=="
    },
    {
        "index": 6,
        "title": "jpost.com",
        "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHcbkTxj_dlMn-DI_FwmkLWzEIAY0z6rgto8RuZQ9tG6jHPI4pU55g2gcnhw1Sv0sQ_di4KA2fPzmwx3cdPSTQGhCxhlQk9oChPZDaJr1ANL0XKf6gL1aMKXg--lGdIWeuNzslv9ab03vdG-QRRwlswhBAZTNQF9v6yTwWlpbY="
    },
    {
        "index": 7,
        "title": "timesofisrael.com",
        "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFmGGTYxS2SSYeHOgXWIWqGpjFQy3L6IGyoK2wI_d66noPr-09ATUc2VFxOxn92JNDJQGtw4klEow7DVdYujt0-XhFbGpz5MZnTvXDjIOvsWwDepAYUhbzH1XdsLx5T8O5sPDO1q0yXx_kdPvBOF05GRhQ="
    }
]

# Resolution results from actual scraping
sess1_resolutions = {
    0: ("https://www.gov.il/en/pages/spoke-statement251023", 3),
    1: ("https://www.gov.il/en/pages/statement-by-pm-netanyahu-28-oct-2023", 4),
    2: ("https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEfbyLCzt72j8Qg9N_dMyFYAc9SDF9pmPwlLhcdpENTHTv2ca1A8-W1z7HvDA3jUyJubPhqO9TeKRzmkkLVP7GGxwmTBoVJcTggUOPgTyl5uiZhufXnbKKkeLcwUfSpQ3COo1psR_AWon61weMHz3QkUNpLxDIE37La5-ese22KaKYLP9Sfhob0nqooBlj9KUtjX43T1usRWsejAiN_rxROnTk1ivFoYw==", 28),
    3: ("https://www.aljazeera.com/news/2023/10/29/netanyahu-says-war-entering-second-stage-as-troops-push-into-gaza", 15),
    4: ("https://www.theguardian.com/world/2023/oct/28/netanyahu-declares-a-second-war-of-independence-as-fears-for-gazans-grow", 7),
    5: ("https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFyO3ZKC3V7gEsScKa6jB_w6gac6ntwOkxKrSI27wWg6Dq9K9XcsMQTIb8L4QdGcqyyXiRlKqFeasG0Akt4oroL0_jmkBPEBh2eeqr_Fbv_kv18srKhBZd_6ryEZ6Zi1zM3jnx8K3jaauUsF9GBLAoqegPBa1MHLTNVvJt5HfbpvOKr6gzKsOIUkeyvfm1Vw0kVL4_6c9lr1NE2Lf2TrW7hY3Br4Q==", 27),
    6: ("https://www.jpost.com/israel-news/benjamin-netanyahu/article-770633", 8),
    7: ("https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFmGGTYxS2SSYeHOgXWIWqGpjFQy3L6IGyoK2wI_d66noPr-09ATUc2VFxOxn92JNDJQGtw4klEow7DVdYujt0-XhFbGpz5MZnTvXDjIOvsWwDepAYUhbzH1XdsLx5T8O5sPDO1q0yXx_kdPvBOF05GRhQ=", 117),
}

sess2_resolutions = {
    0: ("https://www.gov.il/en/pages/spoke-statement251023", 2),
    1: ("https://www.gov.il/en/pages/statement-by-pm-netanyahu-28-oct-2023", 2),
    2: ("https://www.timesofisrael.com/pm-never-again-is-now-its-our-2nd-independence-war-humanitys-war-against-evil/", 28),
    3: ("https://www.aljazeera.com/news/2023/10/29/netanyahu-says-war-entering-second-stage-as-troops-push-into-gaza", 15),
    4: ("https://www.theguardian.com/world/2023/oct/28/netanyahu-declares-a-second-war-of-independence-as-fears-for-gazans-grow", 7),
    5: ("https://www.timesofisrael.com/meeting-netanyahu-families-of-gaza-hostages-urge-all-for-all-prisoner-swap/", 27),
    6: ("https://www.jpost.com/israel-news/benjamin-netanyahu/article-770633", 8),
    7: ("https://www.timesofisrael.com/liveblog-october-28-2023/", 117),
}

def get_domain(url):
    """Extract domain from URL."""
    if url.startswith('http'):
        parts = url.split('/')
        if len(parts) > 2:
            return parts[2]
    return url[:50]

print("="*100)
print("URL RESOLUTION COMPARISON")
print("="*100)
print("\nComparing how Google Vertex AI Search redirect URLs resolved to final destinations\n")

print(f"{'Idx':<4} {'Original Title':<20} {'Session 1 (local)':<50} {'Chunks':<7} {'Session 2 (external)':<50} {'Chunks':<7} {'Match'}")
print("-"*100)

total_chunks_sess1 = 0
total_chunks_sess2 = 0

for citation in citations:
    idx = citation['index']
    title = citation['title']
    
    url1, chunks1 = sess1_resolutions[idx]
    url2, chunks2 = sess2_resolutions[idx]
    
    domain1 = get_domain(url1)
    domain2 = get_domain(url2)
    
    match = "✅" if domain1 == domain2 else "⚠️"
    
    print(f"{idx:<4} {title:<20} {domain1:<50} {chunks1:<7} {domain2:<50} {chunks2:<7} {match}")
    
    total_chunks_sess1 += chunks1
    total_chunks_sess2 += chunks2

print("-"*100)
print(f"{'TOTAL':<4} {'':<20} {'':<50} {total_chunks_sess1:<7} {'':<50} {total_chunks_sess2:<7}")
print(f"\nDifference: {abs(total_chunks_sess1 - total_chunks_sess2)} chunks")

print("\n" + "="*100)
print("ANALYSIS")
print("="*100)

print("\n✅ URLs that resolved identically:")
for citation in citations:
    idx = citation['index']
    url1, chunks1 = sess1_resolutions[idx]
    url2, chunks2 = sess2_resolutions[idx]
    domain1 = get_domain(url1)
    domain2 = get_domain(url2)
    
    if domain1 == domain2:
        print(f"  [{idx}] {citation['title']} → {domain1} ({chunks1} chunks)")

print("\n⚠️  URLs that resolved differently:")
for citation in citations:
    idx = citation['index']
    url1, chunks1 = sess1_resolutions[idx]
    url2, chunks2 = sess2_resolutions[idx]
    domain1 = get_domain(url1)
    domain2 = get_domain(url2)
    
    if domain1 != domain2:
        print(f"\n  [{idx}] {citation['title']}:")
        print(f"      Session 1: {domain1}")
        print(f"                 {chunks1} chunks")
        print(f"      Session 2: {domain2}")
        print(f"                 {chunks2} chunks")
        print(f"      Chunk diff: {abs(chunks1 - chunks2)}")

print("\n" + "="*100)
print("ROOT CAUSE")
print("="*100)
print("""
The differences are caused by Google's Vertex AI Search grounding API redirect URLs
resolving to different final destinations at different times.

Three redirect URLs stayed as redirects in Session 1 but resolved to Times of Israel
in Session 2. This is non-deterministic behavior based on:
- Time of resolution
- Geographic location
- Rate limiting
- Session state

For the two www.gov.il URLs, the external session got LESS content (2 chunks vs 3/4),
suggesting potential scraping issues or different page states.
""")
