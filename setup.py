import globus_sdk
import argparse
import os

# Define the Globus transfer client and authenticate with a user's tokens
client = globus_sdk.NativeAppAuthClient('YOUR_CLIENT_ID')
client.oauth2_start_flow()

authorize_url = client.oauth2_get_authorize_url()
print(f'Please go to this URL and login: {authorize_url}')

auth_code = input('Please enter the code you get after login here: ').strip()
token_response = client.oauth2_exchange_code_for_tokens(auth_code)
transfer_token = token_response.by_resource_server['transfer.api.globus.org']['access_token']

tc = globus_sdk.TransferClient(authorizer=globus_sdk.AccessTokenAuthorizer(transfer_token))

# Parse command line arguments
parser = argparse.ArgumentParser(description='Repeat a Globus transfer task')
parser.add_argument('src_dir', help='Source directory path')
parser.add_argument('dst_dir', help='Destination directory path')
args = parser.parse_args()

# Define the source and destination endpoints
src_ep = 'YOUR_SOURCE_ENDPOINT_ID'
dst_ep = 'YOUR_DESTINATION_ENDPOINT_ID'

# Define the transfer data
transfer_data = globus_sdk.TransferData(tc, src_ep, dst_ep, label='Repeat transfer task')

# Add the source and destination directories to the transfer data
transfer_data.add_item(args.src_dir, os.path.join(args.dst_dir, os.path.basename(args.src_dir)), recursive=True)

# Submit the transfer task and print the task ID
task = tc.submit_transfer(transfer_data)
print(f'Task ID: {task["task_id"]}')

