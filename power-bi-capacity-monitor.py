import os
import logging
from datetime import datetime, timedelta
from pytz import timezone

import azure.functions as func
from azure.identity import ClientSecretCredential
from azure.monitor.query import MetricsQueryClient, MetricAggregationType
from azure.mgmt.powerbidedicated import PowerBIDedicatedManagementClient
from azure.mgmt.powerbidedicated.models import DedicatedCapacityUpdateParameters, ResourceSku
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from msrestazure.azure_active_directory import ServicePrincipalCredentials
from azure.storage.blob import BlobServiceClient

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Starting capacity monitoring function.')

        # load local variables
    subscription_id = os.environ.get('AZURE_SUBSCRIPTION_ID')
    resource_group_name = os.environ.get('RESOURCE_GROUP_NAME')
    capacity_name = os.environ.get('CAPACITY_NAME')
    client_id = os.environ.get('AZURE_CLIENT_ID')
    client_secret = os.environ.get('AZURE_CLIENT_SECRET')
    tenant_id = os.environ.get('AZURE_TENANT_ID')
    overload_threshold_up = float(os.environ.get('OVERLOAD_THRESHOLD_UP', 1))
    overload_threshold_down = float(os.environ.get('OVERLOAD_THRESHOLD_DOWN', 0))
    slack_bot_token = os.environ.get('SLACK_BOT_TOKEN')
    slack_channel_id = os.environ.get('SLACK_CHANNEL_ID')
    tz = os.environ.get('TIMEZONE')

    tz = timezone(tz)
    now = datetime.now(tz)

    logging.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")

    # Blob storage settings
    storage_connection_string = os.environ['AzureWebJobsStorage']
    container_name = 'scalingstate'
    blob_name = 'last_scaled_to_a3.txt'

    resource_id = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.PowerBIDedicated/capacities/{capacity_name}"

    # Authentification for MetricsQueryClient with ClientSecretCredential
    try:
        credential_monitor = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        metrics_client = MetricsQueryClient(credential_monitor)
        logging.info("Authenticated MetricsQueryClient successfully.")
    except Exception as e:
        logging.error(f"Authentification error for MetricsQueryClient: {str(e)}")
        return

    # Authentification for PowerBIDedicatedManagementClient with ServicePrincipalCredentials
    try:
        credential_management = ServicePrincipalCredentials(
            client_id=client_id,
            secret=client_secret,
            tenant=tenant_id
        )
        powerbi_client = PowerBIDedicatedManagementClient(credential_management, subscription_id)
        logging.info("Authenticated PowerBIDedicatedManagementClient successfully.")
    except Exception as e:
        logging.error(f"Authentification error for PowerBIDedicatedManagementClient: {str(e)}")
        return

    # Initialize BlobServiceClient
    try:
        blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        try:
            container_client.create_container()
            logging.info(f"Created container '{container_name}'.")
        except Exception:
            # Container already exists
            pass
    except Exception as e:
        logging.error(f"Error initializing BlobServiceClient: {str(e)}")
        return

    # Get current SKU capacity
    try:
        capacity = powerbi_client.capacities.get_details(resource_group_name, capacity_name)
        current_sku = capacity.sku.name
        logging.info(f"Current SKU capacity '{capacity_name}' je {current_sku}.")
    except Exception as e:
        logging.error(f"Error to get capacity details: {str(e)}")
        return

    if now.weekday() > 4 or not (0 <= now.hour < 24):
        logging.info("Function will not run outside of Monday to Friday Sydney time.")
        if current_sku != 'A1':
            if scale_capacity('A1'):
                send_slack_message(
                    f"🔽 Capacity '{capacity_name}' was scaled down to A1 for the weekend."
                )
        return

    # Function to get CPU usage percentage
    def get_cpu_usage():
        try:
            cpu_metrics_data = metrics_client.query_resource(
                resource_id,
                metric_names=["cpu_metric"],
                timespan=timedelta(minutes=5),
                granularity=timedelta(minutes=1),
                aggregations=[MetricAggregationType.AVERAGE]
            )
            cpu_percentage = cpu_metrics_data.metrics[0].timeseries[0].data[-1].average
            logging.info(f"CPU usage is {cpu_percentage}%.")
            return cpu_percentage
        except Exception as e:
            logging.error(f"Error getting CPU metric: {str(e)}")
            return None

    # Get Overload metric
    try:
        metrics_data = metrics_client.query_resource(
            resource_id,
            metric_names=["overload_metric"],
            timespan=timedelta(minutes=1),
            granularity=timedelta(minutes=1),
            aggregations=[MetricAggregationType.AVERAGE]
        )
        overload_value = metrics_data.metrics[0].timeseries[0].data[-1].average
        logging.info(f"Overload metric value is {overload_value}.")
    except Exception as e:
        logging.error(f"Error getting overload metric: {str(e)}")
        return

    # Get CPU usage
    cpu_usage = get_cpu_usage()

    # Scale capacity function
    def scale_capacity(new_sku):
        try:
            capacity_update = DedicatedCapacityUpdateParameters(
                sku=ResourceSku(name=new_sku, tier='PBIE_Azure')
            )
            result = powerbi_client.capacities.update(
                resource_group_name,
                capacity_name,
                capacity_update
            )
            result.result()
            logging.info(f"Capacity '{capacity_name}' have been upsale successfully {new_sku}.")
            return True
        except Exception as e:
            logging.error(f"Scale capacity error: {str(e)}")
            return False

    # Slack messgae
    def send_slack_message(message):
        client = WebClient(token=slack_bot_token)
        try:
            client.chat_postMessage(channel=slack_channel_id, text=message)
            logging.info('Message was successfully sent.')
        except SlackApiError as e:
            logging.error(f'Cannnot send message: {e.response["error"]}')

    # Scaling logic
    if overload_value >= overload_threshold_up:
        if current_sku == 'A1':
            if scale_capacity('A3'):
                send_slack_message(f"🔼 Capacity '{capacity_name}' was scaled up to A3 due to high overload ({overload_value}) and CPU usage ({cpu_usage}%).")
        elif current_sku == 'A3':
            if scale_capacity('A4'):
                send_slack_message(f"🔼 Capacity '{capacity_name}' was scaled up to A4 due to high overload ({overload_value}) and CPU usage ({cpu_usage}%).")
                # Write the current timestamp to blob
                now = datetime.utcnow().isoformat()
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.upload_blob(now, overwrite=True)
        else:
            logging.info('Already at maximum capacity.')
    elif current_sku == 'A4' and overload_value == 0 and cpu_usage == 0.0:
        # Check if 5 minutes have passed since scaling up to A4
        try:
            blob_client = container_client.get_blob_client(blob_name)
            last_scaled_time_bytes = blob_client.download_blob().readall()
            last_scaled_time_str = last_scaled_time_bytes.decode('utf-8')
            last_scaled_time = datetime.fromisoformat(last_scaled_time_str)
            now = datetime.utcnow()
            if (now - last_scaled_time) >= timedelta(minutes=5):
                # Scale down to A1
                if scale_capacity('A1'):
                    send_slack_message(f"🔽 Capacity '{capacity_name}' was scaled down to A1 due to low overload ({overload_value}) and CPU usage ({cpu_usage}%) after 5 minutes.")
                    # Delete the blob
                    blob_client.delete_blob()
            else:
                logging.info('Less than 5 minutes since scaling up to A4, not scaling down yet.')
        except Exception as e:
            logging.error(f"Error reading last scaled time: {str(e)}")
            logging.info('Cannot determine last scaled time, not scaling down.')
    elif overload_value <= overload_threshold_down and current_sku == 'A3':
        if scale_capacity('A1'):
            send_slack_message(f"🔽 Capacity '{capacity_name}' was scaled down to A1 due to low overload ({overload_value}) and CPU usage ({cpu_usage}%).")
    else:
        logging.info('No scaling action required.')
