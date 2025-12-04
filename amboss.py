import requests
import json
import logging

class AmbossAPIError(Exception):
    pass

def fetch_amboss_data(pubkey, api_key, time_range="TODAY", timeout=10):
    amboss_url = "https://api.amboss.space/graphql"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    query = """
        query Fee_info($pubkey: String!, $timeRange: SnapshotTimeRangeEnum) {
            getNode(pubkey: $pubkey) {
                graph_info {
                    channels {
                        fee_info(timeRange: $timeRange) {
                            remote {
                                max
                                mean
                                median
                                weighted
                                weighted_corrected
                            }
                        }
                    }
                }
            }
        }
    """
    variables = {"pubkey": pubkey, "timeRange": time_range}
    payload = {"query": query, "variables": variables}
    try:
        logging.debug(f"Fetching {pubkey} data for {time_range}")
        response = requests.post(
            amboss_url, json=payload, headers=headers, timeout=timeout
        )
        response.raise_for_status()
        data = response.json()
        logging.debug(
            f"Raw Amboss API response for {time_range}: {json.dumps(data)}"
        )
        if data.get("errors"):
            logging.error(f"Amboss API error for {time_range}: {data['errors']}")
            raise AmbossAPIError(
                f"Amboss API error for {time_range}: {data['errors']}"
            )
        node = data.get("data", {}).get("getNode")
        if not node:
            logging.warning(
                f"No node data found for {pubkey} in time range {time_range}"
            )
            return {}
        channels = node.get("graph_info", {}).get("channels")
        fee_info = {}
        if isinstance(channels, dict):
            fee_info = channels.get("fee_info", {}).get("remote", {})
        elif isinstance(channels, list) and channels:
            fee_info = channels[0].get("fee_info", {}).get("remote", {})
        if not fee_info:
            logging.warning(
                f"No channels found for pubkey {pubkey} in time range {time_range}"
            )
            return {}
        return {
            "mean": fee_info.get("mean"),
            "median": fee_info.get("median"),
        }
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Amboss data for {time_range}: {e}")
        raise AmbossAPIError(f"Error fetching Amboss data for {time_range}: {e}")


def fetch_channel_fee_history(channel_id, api_key, time_period="1w", timeout=10):
    """Fetch fee history for a specific channel from Amboss API."""
    amboss_url = "https://api.amboss.space/graphql"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    query = """
        query ChannelFeeInfo($channel_id: String!, $time_period: String!) {
            getChannel(id: $channel_id) {
                id
                short_channel_id
                fee_history(time_period: $time_period) {
                    timestamp
                    fee_rate_milli_msat
                }
            }
        }
    """

    variables = {
        "channel_id": str(channel_id),
        "time_period": time_period
    }

    payload = {"query": query, "variables": variables}

    try:
        logging.debug(f"Fetching channel {channel_id} fee history for {time_period}")
        response = requests.post(
            amboss_url, json=payload, headers=headers, timeout=timeout
        )
        response.raise_for_status()
        data = response.json()

        logging.debug(f"Amboss channel fee history response: {json.dumps(data)}")

        if data.get("errors"):
            error_msg = f"Amboss API error: {data['errors']}"
            logging.error(error_msg)
            raise AmbossAPIError(error_msg)

        channel_data = data.get("data", {}).get("getChannel")
        if not channel_data:
            logging.warning(f"No channel data found for channel_id {channel_id}")
            return {
                "channel_id": channel_id,
                "short_channel_id": None,
                "fee_history": []
            }

        return {
            "channel_id": channel_data.get("id"),
            "short_channel_id": channel_data.get("short_channel_id"),
            "fee_history": channel_data.get("fee_history", [])
        }

    except requests.exceptions.RequestException as e:
        error_msg = f"Error fetching Amboss channel fee history: {e}"
        logging.error(error_msg)
        raise AmbossAPIError(error_msg)
