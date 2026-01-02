import requests
import json


def test_tfl_api():
    """Test TfL API connectivity"""
    base_url = "https://api.tfl.gov.uk"

    # Test 1: Get bus arrivals for a stop
    stop_id = "940GZZLUASL"  # Oxford Circus
    url = f"{base_url}/StopPoint/{stop_id}/Arrivals"

    print(f"Testing API: {url}")
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        print(f"✓ API connected successfully!")
        print(f"✓ Found {len(data)} bus arrivals")

        # Show sample
        if data:
            sample = data[0]
            print(f"\nSample arrival:")
            print(f"  Line: {sample.get('lineName')}")
            print(f"  Destination: {sample.get('destinationName')}")
            print(f"  Time to station: {sample.get('timeToStation')}s")
        return True
    else:
        print(f"✗ API error: {response.status_code}")
        return False


if __name__ == "__main__":
    test_tfl_api()
